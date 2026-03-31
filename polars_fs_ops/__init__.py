"""Polars plugin for file system operations.

Provides Polars expression functions for common file system tasks such as
checking file existence, copying, moving, removing files, and listing
directories. Operations are implemented in Rust for performance.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function

from polars_fs_ops._internal import __version__ as __version__

if TYPE_CHECKING:
    from polars_fs_ops.typing import IntoExprColumn

LIB = Path(__file__).parent


# Operations
def cp_file(from_path: IntoExprColumn, to_path: IntoExprColumn, dry_run: bool = False) -> pl.Expr:
    """Copy files from source paths to destination paths using std::fs.

    Args:
        from_path: Column or expression containing source file paths.
        to_path: Column or expression containing destination file paths.
            Existing directories are rejected.
        dry_run: If True, only simulate the copy without actually performing it.

    Returns:
        A Boolean expression indicating success of each copy operation.
    """
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="cp_file",
        is_elementwise=True,
        kwargs={"dry_run": dry_run},
    )


def mv_file(
    from_path: IntoExprColumn,
    to_path: IntoExprColumn,
    preserve_extension: bool = True,
    dry_run: bool = False,
) -> pl.Expr:
    """Move (rename) files from source paths to destination paths using std::fs.

    Args:
        from_path: Column or expression containing source file paths.
        to_path: Column or expression containing destination file paths or existing
            directories. When a directory is provided, the source filename is appended.
        preserve_extension: If True, require matching file extensions for file-to-file
            moves.
        dry_run: If True, only simulate the move without actually performing it.

    Returns:
        A Boolean expression indicating success of each move operation.
    """
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="mv_file",
        is_elementwise=True,
        kwargs={"preserve_extension": preserve_extension, "dry_run": dry_run},
    )


def rm_file(file_path: IntoExprColumn, dry_run: bool = False) -> pl.Expr:
    """Remove files at the given paths.

    Args:
        file_path: Column or expression containing file paths to remove.
        dry_run: If True, only simulate the removal without actually performing it.

    Returns:
        A Boolean expression indicating success of each removal.
    """
    return register_plugin_function(
        args=[file_path],
        plugin_path=LIB,
        function_name="rm_file",
        is_elementwise=True,
        kwargs={"dry_run": dry_run},
    )


def ls_dir(dir_path: IntoExprColumn) -> pl.Expr:
    """List directory contents as a list of paths.

    Each row returns a ``List[String]`` of full paths in the directory.
    Use ``.explode()`` to expand into individual rows.

    Args:
        dir_path: Column or expression containing directory paths.

    Returns:
        A List(String) expression with directory entries, or null for
        non-existent directories.
    """
    return register_plugin_function(
        args=[dir_path],
        plugin_path=LIB,
        function_name="ls_dir",
        is_elementwise=True,
    )


def uucp_file(
    from_path: IntoExprColumn,
    to_path: IntoExprColumn,
    progress_bar: bool = True,
    dry_run: bool = False,
) -> pl.Expr:
    """Copy files using uutils coreutils (cross-platform GNU cp rewrite).

    Args:
        from_path: Column or expression containing source file paths.
        to_path: Column or expression containing destination file paths or existing
            directories.
        progress_bar: Whether to display a progress bar during copy.
        dry_run: If True, only simulate the copy without actually performing it.

    Returns:
        A Boolean expression indicating success of each copy operation.
    """
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="uucp_file",
        is_elementwise=True,
        kwargs={"progress_bar": progress_bar, "dry_run": dry_run},
    )


def uumv_file(
    from_path: IntoExprColumn,
    to_path: IntoExprColumn,
    preserve_extension: bool = True,
    progress_bar: bool = True,
    dry_run: bool = False,
) -> pl.Expr:
    """Move files using uutils coreutils (cross-platform GNU mv rewrite).

    Args:
        from_path: Column or expression containing source file paths.
        to_path: Column or expression containing destination file paths or existing
            directories.
        preserve_extension: If True, require matching file extensions for file-to-file
            moves.
        progress_bar: Whether to display a progress bar during move.
        dry_run: If True, only simulate the move without actually performing it.

    Returns:
        A Boolean expression indicating success of each move operation.
    """
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="uumv_file",
        is_elementwise=True,
        kwargs={
            "preserve_extension": preserve_extension,
            "progress_bar": progress_bar,
            "dry_run": dry_run,
        },
    )


def cpx_file(
    from_path: IntoExprColumn, to_path: IntoExprColumn, parallel: int = 0, dry_run: bool = False
) -> pl.Expr:
    """Copy files using cpx (high-performance Rust file copying library).

    Args:
        from_path: Column or expression containing source file paths.
        to_path: Column or expression containing destination file paths or existing
            directories.
        parallel: Number of parallel copy threads to request. Use 0 for the library
            default.
        dry_run: If True, only simulate the copy without actually performing it.

    Returns:
        A Boolean expression indicating success of each copy operation.
    """
    if sys.platform != "linux":
        raise NotImplementedError("cpx_file is only supported on Linux")
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="cpx_file",
        is_elementwise=True,
        kwargs={"parallel": parallel, "dry_run": dry_run},
    )


# Checks
def file_exists(file_path: IntoExprColumn) -> pl.Expr:
    """Check whether each file path points to an existing file.

    Args:
        file_path: Column or expression containing file paths.

    Returns:
        A Boolean expression indicating whether each path points to an existing file
        (directories and other non-file paths return False).
    """
    return register_plugin_function(
        args=[file_path],
        plugin_path=LIB,
        function_name="file_exists",
        is_elementwise=True,
    )


def check_valid_parent_dir(file_path: IntoExprColumn) -> pl.Expr:
    """Check whether each file path has a valid parent directory.

    Args:
        file_path: Column or expression containing file paths.

    Returns:
        A Boolean expression indicating whether each path has a valid parent directory.
    """
    return register_plugin_function(
        args=[file_path],
        plugin_path=LIB,
        function_name="check_valid_parent_dir",
        is_elementwise=True,
    )
