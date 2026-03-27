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

## Usage Example

Imagine you have a dataset of downloaded files, and you want to back them up, copy them to a new folder, and clean up the originals — all natively within a Polars lazy/eager computation graph! 

### 1. Setup & Create Test Files

First, let's create a temporary directory populated with some test files.

```python
import tempfile
from pathlib import Path

import polars as pl

from polars_fs_ops import ls_dir, rm_file, uucp_file, uumv_file


pl.Config.set_fmt_str_lengths(150)

# Create a temporary folder and 20 files for testing
temp_dir = tempfile.mkdtemp()
Path(temp_dir).mkdir(parents=True, exist_ok=True)

for i in range(20):
    with open(Path(temp_dir) / f"{i}.txt", "w") as f:
        f.write(f"This is test file {i}\n")
```

### 2. List Directory Contents

We can read the directory contents straight into a DataFrame.

```python
root_dir = str(temp_dir)

df = (
    pl.DataFrame({"source_folder": [root_dir]})
    .with_columns(files=ls_dir(dir_path="source_folder"))
    .select("files")
    .explode("files")
)

print(df.head(3))
```

**Output:**
```text
shape: (3, 1)
┌─────────────────────────┐
│ files                   │
│ ---                     │
│ str                     │
╞═════════════════════════╡
│ /tmp/tmpxd79bk63/19.txt │
│ /tmp/tmpxd79bk63/18.txt │
│ /tmp/tmpxd79bk63/17.txt │
└─────────────────────────┘
```

### 3. Copy (Backup) Files

Now we can copy these files over to a new `backup` directory using `uucp_file`.

```python
backup_dir = Path(temp_dir) / "backup"
backup_dir.mkdir(parents=True, exist_ok=True)

df = df.with_columns(
    bak=uucp_file(
        from_path="files",
        to_path=pl.lit(str(backup_dir)),
        progress_bar=True,
        dry_run=False,
    )
)

print(df.head(3))
```

**Output:**
```text
shape: (3, 2)
┌─────────────────────────┬──────┐
│ files                   ┆ bak  │
│ ---                     ┆ ---  │
│ str                     ┆ bool │
╞═════════════════════════╪══════╡
│ /tmp/tmpxd79bk63/19.txt ┆ true │
│ /tmp/tmpxd79bk63/18.txt ┆ true │
│ /tmp/tmpxd79bk63/17.txt ┆ true │
└─────────────────────────┴──────┘
```
### 4. Create a new folder and move files there

If you want to move or copy data further, you can chain additional operations.

```python
new_folder = Path(temp_dir) / "raw_data"
new_folder.mkdir(parents=True, exist_ok=True)

df_moved = df.with_columns(
    moved=uumv_file(
        from_path="files",
        to_dir=pl.lit(str(new_folder)),
        progress_bar=True,
        dry_run=False,
    )
)

print(df_moved.head(3))
```

**Output:**
```text
shape: (3, 3)
┌─────────────────────────┬──────┬───────┐
│ files                   ┆ bak  ┆ moved │
│ ---                     ┆ ---  ┆ ---   │
│ str                     ┆ bool ┆ bool  │
╞═════════════════════════╪══════╪═══════╡
│ /tmp/tmpxd79bk63/19.txt ┆ true ┆ true  │
│ /tmp/tmpxd79bk63/18.txt ┆ true ┆ true  │
│ /tmp/tmpxd79bk63/17.txt ┆ true ┆ true  │
└─────────────────────────┴──────┴───────┘
```

### 5. Remove Original Files

Finally, we can delete the source files using `rm_file`.

```python
df = df.with_columns(
    removed=rm_file(
        file_path="files",
        dry_run=False,
    )
)

print(df.head(3))
```

**Output:**
```text
shape: (3, 3)
┌─────────────────────────┬──────┬─────────┐
│ files                   ┆ bak  ┆ removed │
│ ---                     ┆ ---  ┆ ---     │
│ str                     ┆ bool ┆ bool    │
╞═════════════════════════╪══════╪═════════╡
│ /tmp/tmpxd79bk63/19.txt ┆ true ┆ true    │
│ /tmp/tmpxd79bk63/18.txt ┆ true ┆ true    │
│ /tmp/tmpxd79bk63/17.txt ┆ true ┆ true    │
└─────────────────────────┴──────┴─────────┘
```