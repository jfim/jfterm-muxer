# Default: lint + test
default: check test

# Autoformat
fmt:
    cargo fmt

# Format check + clippy as hard errors (run before every commit)
check:
    cargo fmt --check
    cargo clippy --all-targets -- -D warnings

# Run the test suite
test:
    cargo test

# Debug build
build:
    cargo build
