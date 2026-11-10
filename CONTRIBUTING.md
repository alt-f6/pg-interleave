# Contributing to pg-interleave

Thank you for your interest in improving `pg-interleave`!

## Development Setup

Requirements:

- Rust 1.88+ (Edition 2024, let-chains enabled)
- Node.js 20+
- PostgreSQL instance running locally on port 5432 (user: `postgres`, password: `postgres`, db: `postgres`)

### Running Checks

Before submitting a pull request, ensure all linters and test suites pass:

```bash
# 1. Rust engine checks
cargo fmt --check
cargo clippy -p pg-interleave-server -- -D warnings
cargo test

# 2. TypeScript SDK checks
cd packages/sdk-ts
npm run build
npm test

# 3. Real-world queue suite
cd ../../examples/pg-boss-exploration
npm test
```

### Guidelines

- **No Silent Failures:** Any new interceptor rule must provide deterministic state reporting via rule IDs and stats.
- **Wire Safety:** Keep protocol decoding strictly zero-alloc where possible using `bytes::BytesMut`.
- **Reproducibility:** If adding a concurrency-related feature, write a test proving both failure and deterministic mitigation.
