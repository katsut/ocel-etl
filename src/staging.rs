//! The loose working representation for ETL.
//!
//! Extraction rarely sees data in a valid order: events may reference objects
//! that arrive on a later page, attribute schemas grow as new fields appear,
//! and the same object is touched by many records. `StagingLog` absorbs that
//! chaos — everything can be added in any order — and [`StagingLog::into_ocel`]
//! is the single validation gate into a spec-conformant [`ocel::Ocel`].

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use ocel::{
    AttrType, AttrValue, AttributeDefinition, Event, EventAttribute, EventType, Object,
    ObjectAttribute, ObjectType, Ocel, Relationship, Violation,
};

/// An event observed during extraction (relationships may dangle until the
/// referenced objects are added).
#[derive(Debug, Clone)]
pub struct StagingEvent {
    pub id: String,
    pub event_type: String,
    pub time: DateTime<Utc>,
    /// Attribute name/value pairs.
    pub attributes: Vec<(String, AttrValue)>,
    /// `E2O` targets as (`object_id`, qualifier).
    pub relations: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
struct StagingObject {
    object_type: String,
    /// (name, value, time) — dynamic attribute observations, any order.
    attributes: Vec<(String, AttrValue, DateTime<Utc>)>,
    /// `O2O` targets as (`target_id`, qualifier).
    relations: Vec<(String, String)>,
}

/// The loose intermediate representation: order-free ingestion, dangling
/// references allowed, schema grows as observed.
#[derive(Debug, Default)]
pub struct StagingLog {
    events: Vec<StagingEvent>,
    /// Insertion-ordered objects (id -> index into `objects`).
    object_index: HashMap<String, usize>,
    object_ids: Vec<String>,
    objects: Vec<StagingObject>,
    /// event type -> attribute -> observed type (first observation wins;
    /// conflicting observations degrade to `String`).
    event_schema: BTreeMap<String, BTreeMap<String, AttrType>>,
    object_schema: BTreeMap<String, BTreeMap<String, AttrType>>,
}

fn observe(schema: &mut BTreeMap<String, AttrType>, name: &str, value: &AttrValue) {
    let observed = value.attr_type();
    match schema.get(name) {
        None => {
            schema.insert(name.to_owned(), observed);
        }
        Some(&declared) if declared != observed => {
            schema.insert(name.to_owned(), AttrType::String);
        }
        Some(_) => {}
    }
}

/// Coerce a value to its (possibly degraded) declared type.
fn conform(value: AttrValue, declared: AttrType) -> AttrValue {
    if value.attr_type() == declared {
        value
    } else {
        AttrValue::String(value.to_text())
    }
}

impl StagingLog {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add an event (any order; its object references may not exist yet).
    pub fn add_event(&mut self, event: StagingEvent) {
        let schema = self
            .event_schema
            .entry(event.event_type.clone())
            .or_default();
        for (name, value) in &event.attributes {
            observe(schema, name, value);
        }
        self.events.push(event);
    }

    /// Ensure an object exists with the given type. Repeated calls merge; a
    /// later non-empty type fills in a placeholder created by references.
    pub fn upsert_object(&mut self, id: &str, object_type: &str) {
        let index = self.object_slot(id);
        if self.objects[index].object_type.is_empty() {
            object_type.clone_into(&mut self.objects[index].object_type);
        }
        self.object_schema
            .entry(object_type.to_owned())
            .or_default();
    }

    /// Record a dynamic attribute observation for an object (created as a
    /// placeholder if unseen).
    pub fn add_object_attribute(
        &mut self,
        id: &str,
        name: &str,
        value: AttrValue,
        time: DateTime<Utc>,
    ) {
        let index = self.object_slot(id);
        let object_type = self.objects[index].object_type.clone();
        if !object_type.is_empty() {
            let schema = self.object_schema.entry(object_type).or_default();
            observe(schema, name, &value);
        }
        self.objects[index]
            .attributes
            .push((name.to_owned(), value, time));
    }

    /// Record an `O2O` relation (either endpoint may not exist yet).
    pub fn add_o2o(&mut self, source_id: &str, target_id: &str, qualifier: &str) {
        let index = self.object_slot(source_id);
        self.objects[index]
            .relations
            .push((target_id.to_owned(), qualifier.to_owned()));
    }

    fn object_slot(&mut self, id: &str) -> usize {
        if let Some(&index) = self.object_index.get(id) {
            return index;
        }
        let index = self.objects.len();
        self.object_index.insert(id.to_owned(), index);
        self.object_ids.push(id.to_owned());
        self.objects.push(StagingObject::default());
        index
    }

    /// The validation gate: convert into a spec-conformant [`Ocel`].
    ///
    /// Late attribute observations are re-checked against the final schema
    /// (values whose type degraded to string are stringified), then everything
    /// is delegated to [`ocel::OcelBuilder`] — dangling references, duplicate
    /// ids, and placeholder objects that never received a type surface as
    /// [`Violation`]s.
    ///
    /// # Errors
    ///
    /// Returns every [`Violation`] found when the staged data does not form a
    /// valid OCEL 2.0 log.
    pub fn into_ocel(self) -> Result<Ocel, Vec<Violation>> {
        let mut builder = Ocel::builder();

        for (name, attrs) in &self.event_schema {
            builder.add_event_type(EventType {
                name: name.clone(),
                attributes: attr_defs(attrs),
            });
        }
        for (name, attrs) in &self.object_schema {
            builder.add_object_type(ObjectType {
                name: name.clone(),
                attributes: attr_defs(attrs),
            });
        }

        for event in self.events {
            let schema = self.event_schema.get(&event.event_type);
            let attributes = event
                .attributes
                .into_iter()
                .map(|(name, value)| {
                    let declared = schema
                        .and_then(|s| s.get(&name).copied())
                        .unwrap_or_else(|| value.attr_type());
                    EventAttribute {
                        value: conform(value, declared),
                        name,
                    }
                })
                .collect();
            builder.add_event(Event {
                id: event.id,
                event_type: event.event_type,
                time: event.time,
                attributes,
                relationships: relationships(event.relations),
            });
        }

        for (id, object) in self.object_ids.into_iter().zip(self.objects) {
            let schema = self.object_schema.get(&object.object_type);
            let mut attributes: Vec<ObjectAttribute> = object
                .attributes
                .into_iter()
                .map(|(name, value, time)| {
                    let declared = schema
                        .and_then(|s| s.get(&name).copied())
                        .unwrap_or_else(|| value.attr_type());
                    ObjectAttribute {
                        value: conform(value, declared),
                        name,
                        time,
                    }
                })
                .collect();
            attributes.sort_by(|a, b| a.time.cmp(&b.time).then_with(|| a.name.cmp(&b.name)));
            let mut relations = object.relations;
            relations.sort();
            relations.dedup();
            builder.add_object(Object {
                id,
                object_type: object.object_type,
                attributes,
                relationships: relationships(relations),
            });
        }

        builder.build()
    }
}

fn attr_defs(attrs: &BTreeMap<String, AttrType>) -> Vec<AttributeDefinition> {
    attrs
        .iter()
        .map(|(name, &value_type)| AttributeDefinition {
            name: name.clone(),
            value_type,
        })
        .collect()
}

fn relationships(pairs: Vec<(String, String)>) -> Vec<Relationship> {
    pairs
        .into_iter()
        .map(|(object_id, qualifier)| Relationship {
            object_id,
            qualifier,
        })
        .collect()
}
