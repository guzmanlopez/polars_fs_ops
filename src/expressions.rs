#![allow(clippy::unused_unit)]
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use cpx::cli::args::CopyOptions;
use cpx::core::copy::copy;
use polars::prelude::arity::broadcast_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

//  Using std::fs for file operations
#[polars_expr(output_type=Boolean)]
fn file_exists(inputs: &[Series]) -> PolarsResult<Series> {
    let fp: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked =
        fp.apply_nonnull_values_generic(DataType::Boolean, |value: &str| Path::new(value).exists());
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn cp_file(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => std::fs::copy(from, to).is_ok(),
                _ => false,
            }
        });
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn mv_file(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => std::fs::rename(from, to).is_ok(),
                _ => false,
            }
        });
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn rm_file(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked = from
        .apply_nonnull_values_generic(DataType::Boolean, |value: &str| {
            std::fs::remove_file(value).is_ok()
        });
    Ok(out.into_series())
}

#[polars_expr(output_type=String)]
fn ls_dir(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let out: StringChunked = from.apply_nonnull_values_generic(DataType::String, |value: &str| {
        std::fs::read_dir(value)
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .map(|entry| entry.path().to_string_lossy().to_string())
                    .collect::<Vec<String>>()
                    .join(", ")
            })
            .unwrap_or_default()
    });
    Ok(out.into_series())
}

// Using uutils: Cross-platform Rust rewrite of the GNU coreutils
#[polars_expr(output_type=Boolean)]
fn uucp_file(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to_dir: &StringChunked = inputs[1].str()?;
    let progress_bar: &BooleanChunked = inputs[2].bool()?;
    let options = uu_cp::Options {
        progress_bar: progress_bar.get(0).unwrap_or(false),
        ..Default::default()
    };

    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to_dir, |from: Option<&str>, to: Option<&str>| match (
            from, to,
        ) {
            (Some(from), Some(to_dir)) => {
                uu_cp::copy(&[PathBuf::from(from)], Path::new(to_dir), &options).is_ok()
            },
            _ => false,
        });
    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn uumv_file(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let progress_bar: &BooleanChunked = inputs[2].bool()?;
    let options = uu_mv::Options {
        progress_bar: progress_bar.get(0).unwrap_or(false),
        ..Default::default()
    };

    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => {
                    uu_mv::mv(&[OsString::from(from), OsString::from(to)], &options).is_ok()
                },
                _ => false,
            }
        });
    Ok(out.into_series())
}

// Using cpx: A Rust library for high-performance file copying with advanced features
#[polars_expr(output_type=Boolean)]
fn cpx_file(inputs: &[Series]) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let parallel: &Int32Chunked = inputs[2].i32()?;
    let mut options = CopyOptions::none();
    options.parallel = parallel.get(0).unwrap_or(0).max(0) as usize;

    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => copy(Path::new(from), Path::new(to), &options).is_ok(),
                _ => false,
            }
        });
    Ok(out.into_series())
}
