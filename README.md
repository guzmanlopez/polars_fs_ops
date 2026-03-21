# polars-fs-ops

Extremely shrimple (but not crab), this Python Polars Rust plugin provides filesystem operations such as copy, move, remove, and list directory, using file paths stored in a Polars DataFrame. It's a wrapper around Rust's standard filesystem APIs (`std::fs`), Rust rewrite of the GNU coreutils (`uutils`) and others.