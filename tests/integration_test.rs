use tracing::{debug_span, info, span, Level};

#[test_log::test]
fn setup_sqlite_backend() {
    let _span = debug_span!("test-main-span").entered();
    let backend = eventstore::backend::sqlite::SqliteBackend::new();
}
