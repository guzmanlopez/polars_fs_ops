# polars-fs-ops

[![Quality checks and tests](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/quality-checks-and-tests.yml/badge.svg?branch=dev)](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/quality-checks-and-tests.yml)
[![CI](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/ci.yml/badge.svg)](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/ci.yml)

Extremely shrimple (but not crab), this Python Polars Rust plugin provides filesystem operations such as copy, move, remove, and list directory, using file paths stored in a Polars DataFrame. It's a wrapper around Rust's standard filesystem APIs (`std::fs`), Rust rewrite of the GNU coreutils (`uutils`) and others.

This plugin exists to bring the same expressive, DataFrame-centric API style that Polars offers to filesystem workflows. It is not mainly about performance improvements, because these operations are mostly I/O-bound; the value is keeping a consistent API around objects that already live in a Polars DataFrame.

## Installation

### From a GitHub Release (pre-built wheel, no Rust needed)

Pre-built wheels are published as GitHub Release assets. Choose the wheel whose platform tag matches your operating system and CPU architecture.

This project requires Python 3.11+, so release wheels use the `cp311-abi3` tag.

`cp311-abi3` does not mean "Python 3.11 only". It means the extension is built against CPython's stable ABI with Python 3.11 as the minimum supported version, so the same wheel can be installed on CPython 3.11, 3.12, and 3.13.

Some releases may also include free-threaded Python 3.14 wheels with a `cp314t` tag. Those are separate artifacts and are not the same as the standard `abi3` wheels.

#### Linux

For glibc-based Linux distributions such as Arch Linux, Ubuntu, Debian, and Fedora, use one of these wheel names:

- `polars_fs_ops-VERSION-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl`
- `polars_fs_ops-VERSION-cp311-abi3-manylinux_2_17_aarch64.manylinux2014_aarch64.whl`

Using `uv` on Linux `x86_64`:

```bash
uv add 'polars_fs_ops @ https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl'
```

Using `pip` on Linux `x86_64`:

```bash
pip install 'https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp311-abi3-manylinux_2_17_x86_64.manylinux2014_x86_64.whl'
```

On Linux `aarch64`, replace the filename with the corresponding `aarch64` wheel.

#### macOS

Use the wheel that matches your Mac architecture:

- Intel Macs: `polars_fs_ops-VERSION-cp311-abi3-macosx_10_12_x86_64.whl`
- Apple Silicon Macs: `polars_fs_ops-VERSION-cp311-abi3-macosx_11_0_arm64.whl`

Using `uv` on macOS Apple Silicon:

```bash
uv add 'polars_fs_ops @ https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp311-abi3-macosx_11_0_arm64.whl'
```

Using `pip` on macOS Apple Silicon:

```bash
pip install 'https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp311-abi3-macosx_11_0_arm64.whl'
```

If you are on an Intel Mac, replace the filename with the `macosx_10_12_x86_64` wheel.

#### Windows

Use the wheel that matches your Windows architecture:

- `x64`: `polars_fs_ops-VERSION-cp311-abi3-win_amd64.whl`
- `x86`: `polars_fs_ops-VERSION-cp311-abi3-win32.whl`
- `ARM64`: `polars_fs_ops-VERSION-cp311-abi3-win_arm64.whl`

Using `uv` on Windows `x64`:

```bash
uv add "polars_fs_ops @ https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp311-abi3-win_amd64.whl"
```

Using `pip` on Windows `x64`:

```bash
pip install "https://github.com/guzmanlopez/polars_fs_ops/releases/download/VERSION/polars_fs_ops-VERSION-cp311-abi3-win_amd64.whl"
```

Replace `VERSION` with the desired release tag, such as `v0.1.0`, and swap in the wheel filename that matches your OS and CPU architecture.

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