# polars-fs-ops

[![Quality checks and tests](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/quality-checks-and-tests.yml/badge.svg?branch=dev)](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/quality-checks-and-tests.yml)
[![CI](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/ci.yml/badge.svg)](https://github.com/guzmanlopez/polars_fs_ops/actions/workflows/ci.yml)

Extremely shrimple (but not crab), this Python Polars Rust plugin provides filesystem operations such as copy, move, remove, and list directory, using file paths stored in a Polars DataFrame. It's a wrapper around Rust's standard filesystem APIs (`std::fs`), Rust rewrite of the GNU coreutils (`uutils`) and others.

This plugin exists to bring the same expressive, DataFrame-centric API style that Polars offers to filesystem workflows. It is not mainly about performance improvements, because these operations are mostly I/O-bound; the value is keeping a consistent API around objects that already live in a Polars DataFrame.

## Installation

### From a GitHub release (pre-built wheel, no Rust needed)

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

## Usage example

Imagine you have an incoming folder of downloaded files and you want to inspect it, build a manifest with modification times, back everything up, move only the tabular files to a curated folder, and clean up logs, all while staying inside a Polars expression workflow.

The snippets below were executed to verify the shown outputs.

### 1. Set up a small, deterministic demo folder

```python
import datetime as dt
import os
import shutil
import tempfile
from pathlib import Path

import polars as pl

from polars_fs_ops import ls_dir, ls_dir_with_mod, rm_file, uucp_file, uumv_file


pl.Config.set_fmt_str_lengths(120)
pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_cols(10)

root = Path(tempfile.mkdtemp(prefix="polars_fs_ops_demo_"))
shutil.rmtree(root, ignore_errors=True)

incoming_dir = root / "incoming"
backup_dir = root / "backup"
curated_tabular_dir = root / "curated" / "tabular"

for directory in [incoming_dir, backup_dir, curated_tabular_dir]:
    directory.mkdir(parents=True, exist_ok=True)

files = [
    (
        "report_2026-04.txt",
        "monthly report\n",
        dt.datetime(2026, 4, 1, 9, 15, tzinfo=dt.timezone.utc),
    ),
    (
        "sales_2026-04.csv",
        "day,total\n2026-04-01,120\n",
        dt.datetime(2026, 4, 1, 10, 30, tzinfo=dt.timezone.utc),
    ),
    (
        "notes.txt",
        "call supplier\n",
        dt.datetime(2026, 4, 2, 8, 0, tzinfo=dt.timezone.utc),
    ),
    (
        "todo.log",
        "refresh cache\n",
        dt.datetime(2026, 4, 2, 11, 45, tzinfo=dt.timezone.utc),
    ),
]

for name, content, modified_at in files:
    path = incoming_dir / name
    path.write_text(content)
    timestamp = modified_at.timestamp()
    os.utime(path, (timestamp, timestamp))
```

### 2. Build a file manifest with modified times

This is where `ls_dir_with_mod` becomes useful: the directory listing is already shaped like a Polars expression, so we can immediately explode it, unnest it, and derive extra columns.

```python
manifest = (
    pl.DataFrame({"dir": [str(incoming_dir)]})
    .select(ls_dir_with_mod("dir").alias("entries"))
    .explode("entries")
    .unnest("entries")
    .with_columns(
        name=pl.col("path").str.split(r"[/\\]").list.last(),
        ext=pl.col("path").str.split(".").list.last(),
    )
    .select("name", "ext", "path", "modified")
    .sort("modified", descending=True)
)

print(manifest)
```

**Output:**
```text
shape: (4, 4)
┌────────────────────┬─────┬────────────────────────────────────────────────────────────┬─────────────────────┐
│ name               ┆ ext ┆ path                                                       ┆ modified            │
│ ---                ┆ --- ┆ ---                                                        ┆ ---                 │
│ str                ┆ str ┆ str                                                        ┆ datetime[μs]        │
╞════════════════════╪═════╪════════════════════════════════════════════════════════════╪═════════════════════╡
│ todo.log           ┆ log ┆ /tmp/polars_fs_ops_readme_demo/incoming/todo.log           ┆ 2026-04-02 11:45:00 │
│ notes.txt          ┆ txt ┆ /tmp/polars_fs_ops_readme_demo/incoming/notes.txt          ┆ 2026-04-02 08:00:00 │
│ sales_2026-04.csv  ┆ csv ┆ /tmp/polars_fs_ops_readme_demo/incoming/sales_2026-04.csv  ┆ 2026-04-01 10:30:00 │
│ report_2026-04.txt ┆ txt ┆ /tmp/polars_fs_ops_readme_demo/incoming/report_2026-04.txt ┆ 2026-04-01 09:15:00 │
└────────────────────┴─────┴────────────────────────────────────────────────────────────┴─────────────────────┘
```

### 3. Back up everything with `uucp_file`

Once the file paths live in a DataFrame, filesystem actions become another expression in the pipeline.

```python
backup_result = (
    manifest.with_columns(
        copied=uucp_file(
            from_path="path",
            to_path=pl.lit(str(backup_dir)),
            progress_bar=False,
            dry_run=False,
        ),
    )
    .select("name", "copied")
    .sort("name")
)

print(backup_result)
```

**Output:**
```text
shape: (4, 2)
┌────────────────────┬────────┐
│ name               ┆ copied │
│ ---                ┆ ---    │
│ str                ┆ bool   │
╞════════════════════╪════════╡
│ notes.txt          ┆ true   │
│ report_2026-04.txt ┆ true   │
│ sales_2026-04.csv  ┆ true   │
│ todo.log           ┆ true   │
└────────────────────┴────────┘
```

### 4. Move only the CSV data into a curated folder

Here the destination is still expression-driven. We stage the destination directory in one `with_columns`, then feed that derived column into `uumv_file` in the next step.

```python
move_result = (
    manifest.filter(pl.col("ext") == "csv")
    .with_columns(destination_dir=pl.lit(str(curated_tabular_dir)))
    .with_columns(
        moved=uumv_file(
            from_path="path",
            to_path="destination_dir",
            preserve_extension=True,
            progress_bar=False,
            dry_run=False,
        ),
    )
    .select("name", "destination_dir", "moved")
)

print(move_result)
```

**Output:**
```text
shape: (1, 3)
┌───────────────────┬────────────────────────────────────────────────┬───────┐
│ name              ┆ destination_dir                                ┆ moved │
│ ---               ┆ ---                                            ┆ ---   │
│ str               ┆ str                                            ┆ bool  │
╞═══════════════════╪════════════════════════════════════════════════╪═══════╡
│ sales_2026-04.csv ┆ /tmp/polars_fs_ops_readme_demo/curated/tabular ┆ true  │
└───────────────────┴────────────────────────────────────────────────┴───────┘
```

### 5. Remove log files from the incoming folder

```python
cleanup_result = (
    manifest.filter(pl.col("ext") == "log")
    .with_columns(removed=rm_file("path", dry_run=False))
    .select("name", "removed")
)

print(cleanup_result)
```

**Output:**
```text
shape: (1, 2)
┌──────────┬─────────┐
│ name     ┆ removed │
│ ---      ┆ ---     │
│ str      ┆ bool    │
╞══════════╪═════════╡
│ todo.log ┆ true    │
└──────────┴─────────┘
```

### 6. Inspect the resulting folders with `ls_dir`

After the copy, move, and cleanup steps, a plain `ls_dir` is enough to verify the resulting state.

```python
incoming_now = (
    pl.DataFrame({"dir": [str(incoming_dir)]})
    .with_columns(entries=ls_dir("dir"))
    .select("entries")
    .explode("entries")
    .with_columns(name=pl.col("entries").str.split("/").list.last())
    .select("name")
    .sort("name")
)

backup_now = (
    pl.DataFrame({"dir": [str(backup_dir)]})
    .with_columns(entries=ls_dir("dir"))
    .select("entries")
    .explode("entries")
    .with_columns(name=pl.col("entries").str.split("/").list.last())
    .select("name")
    .sort("name")
)

curated_now = (
    pl.DataFrame({"dir": [str(curated_tabular_dir)]})
    .with_columns(entries=ls_dir("dir"))
    .select("entries")
    .explode("entries")
    .with_columns(name=pl.col("entries").str.split("/").list.last())
    .select("name")
    .sort("name")
)

print(incoming_now)
print(backup_now)
print(curated_now)
```

**Output (`incoming_now`):**
```text
shape: (2, 1)
┌────────────────────┐
│ name               │
│ ---                │
│ str                │
╞════════════════════╡
│ notes.txt          │
│ report_2026-04.txt │
└────────────────────┘
```

**Output (`backup_now`):**
```text
shape: (4, 1)
┌────────────────────┐
│ name               │
│ ---                │
│ str                │
╞════════════════════╡
│ notes.txt          │
│ report_2026-04.txt │
│ sales_2026-04.csv  │
│ todo.log           │
└────────────────────┘
```

**Output (`curated_now`):**
```text
shape: (1, 1)
┌───────────────────┐
│ name              │
│ ---               │
│ str               │
╞═══════════════════╡
│ sales_2026-04.csv │
└───────────────────┘
```