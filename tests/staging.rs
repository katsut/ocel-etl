use chrono::{DateTime, Utc};
use ocel::{AttrType, AttrValue, Violation};
use ocel_etl::{StagingEvent, StagingLog};

fn ts(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn event(id: &str, ty: &str, secs: i64, relations: Vec<(&str, &str)>) -> StagingEvent {
    StagingEvent {
        id: id.into(),
        event_type: ty.into(),
        time: ts(secs),
        attributes: vec![],
        relations: relations
            .into_iter()
            .map(|(o, q)| (o.into(), q.into()))
            .collect(),
    }
}

/// Events may reference objects that are only added later (pagination order).
#[test]
fn out_of_order_ingestion_builds_valid_log() {
    let mut staging = StagingLog::new();
    // event arrives first, referencing a not-yet-seen object
    staging.add_event(event("e1", "status_changed", 100, vec![("task-1", "task")]));
    // the object arrives on a "later page"
    staging.upsert_object("task-1", "task");
    staging.add_object_attribute("task-1", "status", AttrValue::String("Open".into()), ts(0));

    let ocel = staging.into_ocel().unwrap();
    assert_eq!(ocel.validate(), Ok(()));
    assert_eq!(ocel.events.len(), 1);
    assert_eq!(ocel.objects.len(), 1);
}

/// References that never resolve surface as violations at the gate.
#[test]
fn dangling_reference_is_rejected() {
    let mut staging = StagingLog::new();
    staging.add_event(event("e1", "status_changed", 100, vec![("ghost", "task")]));

    let violations = staging.into_ocel().unwrap_err();
    assert!(violations.contains(&Violation::DanglingE2O {
        event: "e1".into(),
        object: "ghost".into(),
    }));
}

/// An O2O target that never materializes is rejected at the gate.
#[test]
fn unresolved_o2o_target_is_rejected() {
    let mut staging = StagingLog::new();
    staging.upsert_object("task-1", "task");
    staging.add_o2o("task-1", "task-2", "parent of");

    let violations = staging.into_ocel().unwrap_err();
    assert!(violations.contains(&Violation::DanglingO2O {
        source_id: "task-1".into(),
        target_id: "task-2".into(),
    }));
}

/// An object seen only through attribute observations (never typed via
/// `upsert_object`) is rejected as an untyped placeholder.
#[test]
fn untyped_placeholder_object_is_rejected() {
    let mut staging = StagingLog::new();
    staging.add_object_attribute("task-2", "status", AttrValue::String("Open".into()), ts(0));

    let violations = staging.into_ocel().unwrap_err();
    assert!(violations.iter().any(
        |v| matches!(v, Violation::UndeclaredObjectType { object, .. } if object == "task-2")
    ));
}

/// The attribute schema grows as new fields appear on later records.
#[test]
fn schema_grows_across_events() {
    let mut staging = StagingLog::new();
    staging.add_event(StagingEvent {
        attributes: vec![("changer".into(), AttrValue::String("Alice".into()))],
        ..event("e1", "status_changed", 100, vec![])
    });
    staging.add_event(StagingEvent {
        attributes: vec![("retries".into(), AttrValue::Integer(2))],
        ..event("e2", "status_changed", 200, vec![])
    });

    let ocel = staging.into_ocel().unwrap();
    let declared = &ocel.event_types[0].attributes;
    assert_eq!(declared.len(), 2);
    assert!(declared
        .iter()
        .any(|a| a.name == "changer" && a.value_type == AttrType::String));
    assert!(declared
        .iter()
        .any(|a| a.name == "retries" && a.value_type == AttrType::Integer));
}

/// Conflicting type observations degrade the declaration (and the values) to string.
#[test]
fn conflicting_types_degrade_to_string() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.add_object_attribute("t1", "estimate", AttrValue::Integer(3), ts(0));
    staging.add_object_attribute("t1", "estimate", AttrValue::String("3.5d".into()), ts(10));

    let ocel = staging.into_ocel().unwrap();
    let declared = &ocel.object_types[0].attributes;
    assert!(declared
        .iter()
        .any(|a| a.name == "estimate" && a.value_type == AttrType::String));
    let values: Vec<_> = ocel.objects[0]
        .attributes
        .iter()
        .map(|a| &a.value)
        .collect();
    assert_eq!(
        values,
        vec![
            &AttrValue::String("3".into()),
            &AttrValue::String("3.5d".into())
        ]
    );
}

/// Duplicate O2O observations collapse; provenance rides along as an attribute.
#[test]
fn o2o_dedup_and_provenance() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.upsert_object("t2", "task");
    staging.add_o2o("t1", "t2", "parent of");
    staging.add_o2o("t1", "t2", "parent of"); // seen again on another page
    staging.add_event(StagingEvent {
        attributes: vec![("_source".into(), AttrValue::String("rule".into()))],
        ..event("e1", "task_created", 100, vec![("t1", "created task")])
    });

    let ocel = staging.into_ocel().unwrap();
    assert_eq!(ocel.o2o().count(), 1);
    assert_eq!(
        ocel.events[0].attributes[0].value,
        AttrValue::String("rule".into())
    );
    assert_eq!(ocel.validate(), Ok(()));
}

/// The gated log round-trips through the ocel crate's I/O.
#[test]
fn gated_log_round_trips_through_ocel_io() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.add_object_attribute("t1", "status", AttrValue::String("Open".into()), ts(0));
    staging.add_event(event(
        "e1",
        "task_created",
        100,
        vec![("t1", "created task")],
    ));

    let ocel_log = staging.into_ocel().unwrap();
    let path = std::env::temp_dir().join("ocel-etl-staging-roundtrip.sqlite");
    ocel::io::sqlite::write_path(&ocel_log, &path).unwrap();
    let back = ocel::io::sqlite::read_path(&path).unwrap();
    assert_eq!(ocel_log, back);
    let _ = std::fs::remove_file(&path);
}

/// Once gated, re-staging and re-gating reproduces the log unchanged
/// (the basis for incremental sync).
#[test]
fn from_ocel_round_trips_after_first_gate() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.add_object_attribute("t1", "status", AttrValue::String("Open".into()), ts(0));
    staging.add_object_attribute("t1", "estimate", AttrValue::Integer(3), ts(0));
    staging.upsert_object("t2", "task");
    staging.add_o2o("t1", "t2", "parent of");
    staging.add_event(StagingEvent {
        attributes: vec![("changer".into(), AttrValue::String("Alice".into()))],
        ..event("e1", "status_changed", 100, vec![("t1", "task")])
    });
    let first = staging.into_ocel().unwrap();

    let second = StagingLog::from_ocel(first.clone()).into_ocel().unwrap();
    assert_eq!(first, second);

    // and merging additional data on top still works
    let mut merged = StagingLog::from_ocel(first);
    merged.add_event(event("e2", "status_changed", 200, vec![("t2", "task")]));
    let log = merged.into_ocel().unwrap();
    assert_eq!(log.events.len(), 2);
    assert_eq!(log.validate(), Ok(()));
}

/// Applying an alias table: two ids become one canonical object, references
/// follow, appended duplicate observations collapse at the gate.
#[test]
fn map_object_ids_merges_aliases_and_rewrites_references() {
    let mut staging = StagingLog::new();
    staging.upsert_object("alice@corp", "user");
    staging.upsert_object("@alice", "user");
    staging.upsert_object("task-1", "task");
    staging.add_object_attribute(
        "alice@corp",
        "team",
        AttrValue::String("core".into()),
        ts(0),
    );
    staging.add_object_attribute("@alice", "team", AttrValue::String("core".into()), ts(0));
    staging.add_o2o("task-1", "@alice", "assigned to");
    staging.add_event(event("e1", "commented", 100, vec![("alice@corp", "actor")]));
    staging.add_event(event("e2", "commented", 200, vec![("@alice", "actor")]));

    staging.map_object_ids(|id| {
        if id == "alice@corp" || id == "@alice" {
            "user:alice".into()
        } else {
            id.to_owned()
        }
    });

    let log = staging.into_ocel().unwrap();
    assert_eq!(log.validate(), Ok(()));
    assert_eq!(log.objects.len(), 2); // user:alice + task-1
    let alice = log.objects.iter().find(|o| o.id == "user:alice").unwrap();
    assert_eq!(alice.object_type, "user");
    // the two identical (name, value, time) observations merged into one
    assert_eq!(alice.attributes.len(), 1);
    // O2O and both E2O references were re-pointed
    assert!(log
        .o2o()
        .any(|r| r.source_id == "task-1" && r.target_id == "user:alice"));
    assert!(log
        .events
        .iter()
        .all(|e| e.relationships.iter().all(|r| r.object_id == "user:alice")));
}

/// A placeholder (reference-only) id merging into a typed object keeps the
/// non-empty type regardless of merge order.
#[test]
fn map_object_ids_keeps_the_first_non_empty_type() {
    let mut staging = StagingLog::new();
    // "ghost" exists only as a reference target (placeholder, empty type)
    staging.add_o2o("task-1", "ghost", "related to");
    staging.upsert_object("task-1", "task");
    staging.upsert_object("real", "user");

    staging.map_object_ids(|id| {
        if id == "ghost" || id == "real" {
            "user:merged".into()
        } else {
            id.to_owned()
        }
    });

    let log = staging.into_ocel().unwrap();
    let merged = log.objects.iter().find(|o| o.id == "user:merged").unwrap();
    assert_eq!(merged.object_type, "user");
}

/// Renaming an event type via `map_events` replaces the old declaration.
#[test]
fn map_events_rename_updates_the_schema() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.add_event(event("e1", "labeled", 100, vec![("t1", "subject")]));
    staging.add_event(event("e2", "unlabeled", 200, vec![("t1", "subject")]));

    staging.map_events(|e| {
        if e.event_type == "labeled" || e.event_type == "unlabeled" {
            e.event_type = "triage".into();
        }
    });

    let log = staging.into_ocel().unwrap();
    assert!(log.events.iter().all(|e| e.event_type == "triage"));
    let names: Vec<&str> = log.event_types.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["triage"]);
}

/// Dropping the last event of a type removes the type from the log.
#[test]
fn retain_events_drops_empty_types() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.add_event(event("e1", "comment", 100, vec![("t1", "subject")]));
    staging.add_event(event("e2", "close", 200, vec![("t1", "subject")]));

    staging.retain_events(|e| e.event_type != "comment");

    let log = staging.into_ocel().unwrap();
    assert_eq!(log.events.len(), 1);
    let names: Vec<&str> = log.event_types.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, vec!["close"]);
}

/// Same value re-observed at a different time is a real change, not a dupe.
#[test]
fn dedupe_keeps_distinct_times_of_the_same_value() {
    let mut staging = StagingLog::new();
    staging.upsert_object("t1", "task");
    staging.add_object_attribute("t1", "status", AttrValue::String("open".into()), ts(0));
    staging.add_object_attribute("t1", "status", AttrValue::String("open".into()), ts(50));
    staging.add_object_attribute("t1", "status", AttrValue::String("open".into()), ts(0));

    let log = staging.into_ocel().unwrap();
    let task = log.objects.iter().find(|o| o.id == "t1").unwrap();
    assert_eq!(task.attributes.len(), 2); // ts(0) duplicate collapsed, ts(50) kept
}
