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

Imagine you have a dataset of downloaded image files, and you want to back them up, check if they exist, move them to an archive folder, and finally list the contents of that archive folder — all natively within a Polars lazy/eager computation graph! 

```python
import polars as pl
import polars_fs_ops as plfs

# 1. Dataframe of file operations for an image dataset
df = pl.DataFrame(
    {
        "file_path": [
            "train/img_01.jpg",
            "train/img_02.jpg",
            "test/img_03.jpg",
            "val/img_04.jpg",
            "missing/img_05.jpg",
        ],
    }
)

# 2. Perform filesystem operations as expressions!
result = (
    df
    # Check if the source files actually exist
    .with_columns(file_exists=plfs.file_exists(pl.col("file_path")))
    # Only if it exists, copy it to the backup location
    .with_columns(
        copy_status=pl.when(pl.col("file_exists"))
        .then(
            plfs.uucp_file(
                from_path=pl.col("file_path"),
                to_path=pl.lit("backups/") + pl.col("file_path"),
                progress_bar=True,
            )
        )
        .otherwise(pl.lit(None))
    )
    # Move the backed-up files to an archive directory
    .with_columns(
        move_status=pl.when(pl.col("copy_status"))
        .then(
            plfs.uumv_file(
                from_path=pl.lit("backups/") + pl.col("file_path"),
                to_dir=pl.col("archive_dir"),
                progress_bar=True,
            )
        )
        .otherwise(pl.lit(None))
    )
)

print(result)

# 3. List the contents of the archive directories
dir_contents = (
    pl.DataFrame({"target_dir": ["archive/train/", "archive/test/", "archive/val/"]})
    .with_columns(files=plfs.ls_dir(dir_path=pl.col("target_dir")))
    .explode("files")
)

print(dir_contents)
```