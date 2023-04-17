test:
	RUST_LOG_SPAN_EVENTS=full RUST_LOG=warn cargo test --test integration_test -- --nocapture
