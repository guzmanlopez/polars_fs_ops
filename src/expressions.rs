#![allow(clippy::unused_unit)]
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use cpx::cli::args::CopyOptions;
use cpx::core::copy::copy;
use polars::prelude::arity::broadcast_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

#[derive(Deserialize)]
struct AddProgressBarKwargs {
    progress_bar: bool,
}

#[derive(Deserialize)]
struct AddParallelKwargs {
    parallel: i32,
}

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

fn ls_dir_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(
        "ls_dir".into(),
        DataType::List(Box::new(DataType::String)),
    ))
}

#[polars_expr(output_type_func=ls_dir_output)]
fn ls_dir(inputs: &[Series]) -> PolarsResult<Series> {
    let ca: &StringChunked = inputs[0].str()?;
    let mut builder = ListStringChunkedBuilder::new("ls_dir".into(), ca.len(), ca.len() * 5);

    for opt_val in ca.into_iter() {
        match opt_val {
            Some(dir_path) => match std::fs::read_dir(dir_path) {
                Ok(entries) => {
                    let paths: Vec<String> = entries
                        .filter_map(Result::ok)
                        .map(|entry| entry.path().to_string_lossy().to_string())
                        .collect();
                    builder.append_values_iter(paths.iter().map(|s| s.as_str()));
                },
                Err(_) => builder.append_null(),
            },
            None => builder.append_null(),
        }
    }

    let out = builder.finish();
    Ok(out.into_series())
}

// Using uutils: Cross-platform Rust rewrite of the GNU coreutils
#[polars_expr(output_type=Boolean)]
fn uucp_file(inputs: &[Series], kwargs: AddProgressBarKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to_dir: &StringChunked = inputs[1].str()?;
    let progress_bar: bool = kwargs.progress_bar;
    let options = uu_cp::Options {
        progress_bar: progress_bar,
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
fn uumv_file(inputs: &[Series], kwargs: AddProgressBarKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let progress_bar: bool = kwargs.progress_bar;
    let options = uu_mv::Options {
        progress_bar: progress_bar,
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
fn cpx_file(inputs: &[Series], kwargs: AddParallelKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let parallel: i32 = kwargs.parallel;
    let mut options = CopyOptions::none();
    options.parallel = parallel.max(0) as usize;

    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => copy(Path::new(from), Path::new(to), &options).is_ok(),
                _ => false,
            }
        });
    Ok(out.into_series())
}
