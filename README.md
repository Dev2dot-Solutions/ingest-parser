# ingest-parser

Native tree-sitter parse engine for the dev2.solutions code ingestion pipeline.

A high-performance Rust binary that parses source files using the native
`tree-sitter` Rust crate, processes them in parallel via `rayon`, and outputs
structured JSON. Designed to be called as a subprocess by the `dev2-knowledge`
Go service.

## Why Rust?

Tree-sitter is a **Rust-native** library. Every other binding (Go/CGo,
JavaScript/WASM) adds FFI overhead. The Rust crate provides zero-overhead,
zero-copy access to the parser — no WASM sandbox, no CGo boundary, no
string marshalling. Combined with `rayon` for work-stealing parallelism,
this gives 15-30x performance over the current sequential WASM-based
TypeScript implementation.

## Architecture

```
dev2-knowledge (Go) ──stdin──▶ ingest-parser (Rust)
      │                           │
      │                    ┌──────┴──────┐
      │                    │  rayon pool  │
      │                    │  (parallel) │
      │                    └──────┬──────┘
      │                           │
      │                     stdout (JSON)
      │                           │
      ▼                           ▼
  MongoDB batch insert      Parse results
```

## Usage

```bash
# From the command line
find /path/to/repo -name "*.ts" -o -name "*.go" | ingest-parser > results.json

# From Go
cmd := exec.Command("ingest-parser")
cmd.Stdin = strings.NewReader(filePaths)
output, _ := cmd.Output()
var results []ParseResult
json.Unmarshal(output, &results)
```

## Supported Languages

| Language | Extensions | Entities Extracted |
|----------|-----------|-------------------|
| TypeScript | .ts | functions, classes, imports, calls |
| TSX | .tsx | functions, classes, imports, calls |
| JavaScript | .js, .jsx, .mjs, .cjs | functions, classes, imports, calls |
| Kotlin | .kt, .kts | functions, classes, imports, calls |
| Go | .go | functions, imports, calls |
| HTML | .html, .htm | tracked (no entity extraction) |
| CSS | .css, .scss | tracked (no entity extraction) |

## Output Format

```json
[
  {
    "path": "src/main.ts",
    "functions": [
      {"name": "main", "signature": "async function main()", "line_start": 1, "line_end": 30, "doc_comment": "/** Entry point */"}
    ],
    "classes": [
      {"name": "AppService", "parent_class": null, "interfaces": ["Injectable"]}
    ],
    "imports": [
      {"source_entity": "Component", "target_entity": "@angular/core"}
    ],
    "calls": [
      {"caller_name": "main", "callee_name": "bootstrap"}
    ],
    "error": null,
    "duration_us": 1523
  }
]
```

## Build

```bash
# Requires Rust 1.81+
cargo build --release

# Static musl binary (for Alpine/scratch Docker)
cargo build --release --target x86_64-unknown-linux-musl

# Result
./target/release/ingest-parser
# Binary size: ~5MB
```

## Performance

Target: parse 500 files in < 3 seconds (vs ~60s for the current sequential
TypeScript/WASM implementation with individual DB saves).

Key optimisations:
- **Native tree-sitter**: zero FFI, zero-copy query captures
- **Rayon parallelism**: work-stealing across all CPU cores
- **Pre-compiled queries**: S-expressions compiled once at startup
- **Static binary**: ~5MB, no runtime dependencies
