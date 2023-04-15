use eventstore::backend::{model::Event, sqlite::SqliteBackend};
use r2d2_sqlite::SqliteConnectionManager;
use tracing::debug_span;

/// Helper method
///
/// # Panics
///
/// Panics if .

fn assert_get_aggreate_since_version_of_len(
    aggregate_id: uuid::Uuid,
    since_version: u32,
    backend: &SqliteBackend,
    expected_len: usize,
) {
    let res = backend.get_aggretate_with_opts(
        aggregate_id,
        &eventstore::backend::sqlite::GetAggOpts {
            agg_id: aggregate_id,
            since_version,
        },
    );
    match res {
        Ok(events) => {
            assert!(
                events.len() == expected_len,
                "result len should be {}, but is {}",
                expected_len,
                events.len()
            );
            assert_gap_less_version(&events);
        }
        Err(err) => panic!("something went wrong: {}", err),
    };
}

fn assert_get_aggreate_of_len(
    aggregate_id: uuid::Uuid,
    backend: &SqliteBackend,
    expected_len: usize,
) {
    let res = backend.get_aggretate(aggregate_id);
    match res {
        Ok(events) => {
            assert!(
                events.len() == expected_len,
                "result len should be {}, but is {}",
                expected_len,
                events.len()
            );
            assert_gap_less_version(&events);
        }
        Err(_err) => panic!("something went wrong"),
    };
}

fn assert_gap_less_version(events: &Vec<Event>) {
    let mut last_version = 0;
    for (idx, ev) in events.iter().enumerate() {
        if last_version == 0 {
            last_version = ev.version;
        } else {
            assert!(
                ev.version == last_version + 1,
                "expected version to be gapless, previous version {}, next version {}, expected: {}, at index: {}",
                last_version,
                ev.version,
                last_version + 1,
                idx
            );
            last_version = ev.version;
        }
    }
}

#[test_log::test]
fn setup_sqlite_backend_and_fetch_empty() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("6018b301-a70f-4c00-a362-b2f35dfd611a").unwrap();
    assert_get_aggreate_of_len(aggregate_id, &backend, 0);
}

#[test_log::test]
fn fetch_empty_then_insert_multiple_and_retrieve_list_using_since_version_2() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("d37aaaf7-45a7-4823-83f1-9aae13a6dfd1").unwrap();
    assert_get_aggreate_since_version_of_len(aggregate_id, 0, &backend, 0);
    for i in 1..=10 {
        let event = Event {
            id: aggregate_id,
            version: i,
            data: vec![],
        };
        let _res = backend.append_event(&event).unwrap();
    }
    assert_get_aggreate_since_version_of_len(aggregate_id, 2, &backend, 8);
}

#[test_log::test]
fn fetch_empty_then_insert_multiple_and_retrieve_list_using_since_version_9() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("d37aaaf7-45a7-4823-83f1-9aae13a6dfd1").unwrap();
    assert_get_aggreate_since_version_of_len(aggregate_id, 0, &backend, 0);
    for i in 1..=10 {
        let event = Event {
            id: aggregate_id,
            version: i,
            data: vec![],
        };
        let _res = backend.append_event(&event).unwrap();
    }
    assert_get_aggreate_since_version_of_len(aggregate_id, 9, &backend, 1);
}

#[test_log::test]
fn fetch_empty_then_insert_multiple_and_retrieve_list_using_since_version_1() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("d37aaaf7-45a7-4823-83f1-9aae13a6dfd1").unwrap();
    assert_get_aggreate_since_version_of_len(aggregate_id, 0, &backend, 0);
    for i in 1..=10 {
        let event = Event {
            id: aggregate_id,
            version: i,
            data: vec![],
        };
        let _res = backend.append_event(&event).unwrap();
    }
    assert_get_aggreate_since_version_of_len(aggregate_id, 1, &backend, 9);
}

#[test_log::test]
fn fetch_empty_then_insert_multiple_and_retrieve_list() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("d37aaaf7-45a7-4823-83f1-9aae13a6dfd1").unwrap();
    assert_get_aggreate_of_len(aggregate_id, &backend, 0);
    for i in 1..=10 {
        let event = Event {
            id: aggregate_id,
            version: i,
            data: vec![],
        };
        let _res = backend.append_event(&event).unwrap();
    }
    assert_get_aggreate_of_len(aggregate_id, &backend, 10);
}

#[test_log::test]
fn fetch_empty_then_insert() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("6018b301-a70f-4c00-a362-b2f35dfd611a").unwrap();
    assert_get_aggreate_of_len(aggregate_id, &backend, 0);
    let event = Event {
        id: aggregate_id,
        version: 1,
        data: vec![],
    };
    let _res = backend.append_event(&event).unwrap();
    assert_get_aggreate_of_len(aggregate_id, &backend, 1);
}

#[test_log::test]
fn fetch_empty_then_insert_with_conflicting_version() {
    let _span = debug_span!("test-main-span").entered();
    let manager = SqliteConnectionManager::memory();
    let backend = eventstore::backend::sqlite::SqliteBackend::new(manager);
    let aggregate_id = uuid::Uuid::parse_str("ab27bed6-0e70-49f3-b15b-b211aa767242").unwrap();
    assert_get_aggreate_of_len(aggregate_id, &backend, 0);
    let event = Event {
        id: aggregate_id,
        version: 2,
        data: vec![],
    };
    let res = backend.append_event(&event);
    assert!(res.is_err(), "expected Err but got Ok");
}
