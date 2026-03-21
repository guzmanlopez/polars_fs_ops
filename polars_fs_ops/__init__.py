from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
from polars.plugins import register_plugin_function

from polars_fs_ops._internal import __version__ as __version__

if TYPE_CHECKING:
    from polars_fs_ops.typing import IntoExprColumn

LIB = Path(__file__).parent


def file_exists(file_path: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[file_path],
        plugin_path=LIB,
        function_name="file_exists",
        is_elementwise=True,
    )


def cp_file(from_path: IntoExprColumn, to_path: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="cp_file",
        is_elementwise=True,
    )


def mv_file(from_path: IntoExprColumn, to_path: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[from_path, to_path],
        plugin_path=LIB,
        function_name="mv_file",
        is_elementwise=True,
    )


def rm_file(file_path: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[file_path],
        plugin_path=LIB,
        function_name="rm_file",
        is_elementwise=True,
    )


def ls_dir(dir_path: IntoExprColumn) -> pl.Expr:
    return register_plugin_function(
        args=[dir_path],
        plugin_path=LIB,
        function_name="ls_dir",
        is_elementwise=True,
    )


def uucp_file(
    from_path: IntoExprColumn, to_path: IntoExprColumn, progress_bar: IntoExprColumn
) -> pl.Expr:
    return register_plugin_function(
        args=[from_path, to_path, progress_bar],
        plugin_path=LIB,
        function_name="uucp_file",
        is_elementwise=True,
    )


def uumv_file(
    from_path: IntoExprColumn, to_dir: IntoExprColumn, progress_bar: IntoExprColumn
) -> pl.Expr:
    return register_plugin_function(
        args=[from_path, to_dir, progress_bar],
        plugin_path=LIB,
        function_name="uumv_file",
        is_elementwise=True,
    )


def cpx_file(
    from_path: IntoExprColumn, to_path: IntoExprColumn, parallel: IntoExprColumn
) -> pl.Expr:
    return register_plugin_function(
        args=[from_path, to_path, parallel],
        plugin_path=LIB,
        function_name="cpx_file",
        is_elementwise=True,
    )
