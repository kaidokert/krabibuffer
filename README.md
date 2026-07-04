# krabibuffer

A prototype cross-language shared memory buffer protocol.
## Implementations

- `cpp/` contains the C++20 header-only ABI definition and helpers.
- `rust/` contains the Rust `repr(C, align(8))` implementation and tests.
- `nim/` contains the Nim non-owning shared-memory view with matching header layout and SPSC enqueue/dequeue helpers.
- `go/`, `python/`, and `zig/` contain additional language views over the same wire format.

## Interop checks

The repository includes lightweight ABI interop checks for the Nim-compatible
layout:

```sh
g++ -std=c++20 -Icpp/include cpp/tests/nim_interop.cpp -o /tmp/nim_interop && /tmp/nim_interop
cargo test --manifest-path rust/Cargo.toml
```
