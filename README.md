# polars-fs-ops

[![Quality checks and tests](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/quality-checks-and-tests.yml/badge.svg?branch=dev)](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/quality-checks-and-tests.yml)
[![CI](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/ci.yml/badge.svg)](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/ci.yml)

Extremely shrimple (but not crab), this Python Polars Rust plugin provides filesystem operations such as copy, move, remove, and list directory, using file paths stored in a Polars DataFrame. It's a wrapper around Rust's standard filesystem APIs (`std::fs`), Rust rewrite of the GNU coreutils (`uutils`) and others.

## Installation

### From a GitHub Release (pre-built wheel, no Rust needed)

Using `uv`:

```bash
uv add 'polars_fs_ops @ https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp39-abi3-linux_x86_64.whl'
```

Using `pip`:

```bash
pip install 'https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp39-abi3-linux_x86_64.whl'
```

Replace `VERSION` with the desired release tag (e.g., `v0.1.0`) and choose the appropriate wheel for your platform.

### From source (requires Rust toolchain)

Using `uv`:

```bash
uv add 'polars_fs_ops @ git+https://github.com/guzmanlopez/polars_fs_ops.git@main'
```

Using `pip`:

```bash
pip install 'git+https://github.com/guzmanlopez/polars_fs_ops.git@main'
```

To install a specific version, replace `@main` with a tag (e.g., `@v0.1.0`).

### As a project dependency

In your `pyproject.toml`:

```toml
[project]
dependencies = [
    "polars_fs_ops @ git+https://github.com/guzmanlopez/polars_fs_ops.git@main",
]
```