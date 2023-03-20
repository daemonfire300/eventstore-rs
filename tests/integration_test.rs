use eventstore::backend::{model::Event, sqlite::SqliteBackend};
use tracing::debug_span;

/// Helper method
///
/// # Panics
///
/// Panics if .
fn assert_get_aggreate_of_len(
    aggregate_id: uuid::Uuid,
    backend: &SqliteBackend,
    expected_len: usize,
) {
    let res = backend.get_aggretate(aggregate_id);
    match res {
        Ok(expr) => {
            assert!(
                expr.len() == expected_len,
                "result len should be {}, but is {}",
                expected_len,
                expr.len()
            );
        }
        Err(_err) => panic!("something went wrong"),
    };
}

#[test_log::test]
fn setup_sqlite_backend_and_fetch_empty() {
    let _span = debug_span!("test-main-span").entered();
    let backend = eventstore::backend::sqlite::SqliteBackend::new();
    let aggregate_id = uuid::Uuid::parse_str("6018b301-a70f-4c00-a362-b2f35dfd611a").unwrap();
    assert_get_aggreate_of_len(aggregate_id, &backend, 0);
}

#[test_log::test]
fn fetch_empty_then_insert_multiple_and_retrieve_list() {
    let _span = debug_span!("test-main-span").entered();
    let backend = eventstore::backend::sqlite::SqliteBackend::new();
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
    let backend = eventstore::backend::sqlite::SqliteBackend::new();
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
    let backend = eventstore::backend::sqlite::SqliteBackend::new();
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
