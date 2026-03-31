#![allow(clippy::unused_unit)]
use std::ffi::OsString;
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use cpx::cli::args::CopyOptions;
#[cfg(target_os = "linux")]
use cpx::core::copy::copy;
use polars::prelude::arity::broadcast_binary_elementwise;
use polars::prelude::*;
use pyo3_polars::derive::polars_expr;
use serde::Deserialize;

use crate::utils::{check_file_to_dir, check_file_to_file, check_valid_mv};

//  Using std::fs for operations
#[derive(Deserialize)]
struct CpFileKwargs {
    dry_run: bool,
}

#[polars_expr(output_type=Boolean)]
fn cp_file(inputs: &[Series], kwargs: CpFileKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let dry_run: bool = kwargs.dry_run;
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => {
                    let valid = check_file_to_file(Some(from), Some(to));
                    if !dry_run && valid {
                        std::fs::copy(from, to).is_ok()
                    } else {
                        valid
                    }
                },
                _ => false,
            }
        });
    Ok(out.into_series())
}

#[derive(Deserialize)]
struct MvFileKwargs {
    dry_run: bool,
    preserve_extension: bool,
}

fn resolve_mv_destination(from: &str, to: &str) -> PathBuf {
    let from_path = Path::new(from);
    let to_path = Path::new(to);

    if from_path.is_file() && to_path.is_dir() {
        if let Some(file_name) = from_path.file_name() {
            return to_path.join(file_name);
        }
    }

    to_path.to_path_buf()
}

#[polars_expr(output_type=Boolean)]
fn mv_file(inputs: &[Series], kwargs: MvFileKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let dry_run: bool = kwargs.dry_run;
    let preserve_extension: bool = kwargs.preserve_extension;
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => {
                    let valid = check_valid_mv(Some(from), Some(to), preserve_extension);
                    if !dry_run && valid {
                        let destination = resolve_mv_destination(from, to);
                        std::fs::rename(from, destination).is_ok()
                    } else {
                        valid
                    }
                },
                _ => false,
            }
        });
    Ok(out.into_series())
}

#[derive(Deserialize)]
struct RmFileKwargs {
    dry_run: bool,
}

#[polars_expr(output_type=Boolean)]
fn rm_file(inputs: &[Series], kwargs: RmFileKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let dry_run: bool = kwargs.dry_run;
    let out: BooleanChunked =
        from.apply_nonnull_values_generic(DataType::Boolean, |value: &str| {
            let valid_source = Path::new(value).is_file();
            if !dry_run && valid_source {
                std::fs::remove_file(value).is_ok()
            } else {
                valid_source
            }
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

// Using uutils for operations: Cross-platform Rust rewrite of the GNU coreutils
#[derive(Deserialize)]
struct UuCpKwargs {
    progress_bar: bool,
    dry_run: bool,
}

#[polars_expr(output_type=Boolean)]
fn uucp_file(inputs: &[Series], kwargs: UuCpKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let dry_run: bool = kwargs.dry_run;
    let options = uu_cp::Options {
        progress_bar: kwargs.progress_bar,
        ..Default::default()
    };
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => {
                    let valid = check_file_to_file(Some(from), Some(to))
                        || check_file_to_dir(Some(from), Some(to));
                    if !dry_run && valid {
                        uu_cp::copy(&[PathBuf::from(from)], Path::new(to), &options).is_ok()
                    } else {
                        valid
                    }
                },
                _ => false,
            }
        });
    Ok(out.into_series())
}

#[derive(Deserialize)]
struct UuMvKwargs {
    preserve_extension: bool,
    progress_bar: bool,
    dry_run: bool,
}

#[polars_expr(output_type=Boolean)]
fn uumv_file(inputs: &[Series], kwargs: UuMvKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let dry_run: bool = kwargs.dry_run;
    let preserve_extension: bool = kwargs.preserve_extension;
    let options = uu_mv::Options {
        progress_bar: kwargs.progress_bar,
        ..Default::default()
    };
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => {
                    let valid = check_valid_mv(Some(from), Some(to), preserve_extension);
                    if !dry_run && valid {
                        uu_mv::mv(&[OsString::from(from), OsString::from(to)], &options).is_ok()
                    } else {
                        valid
                    }
                },
                _ => false,
            }
        });
    Ok(out.into_series())
}

// Using cpx: A Rust library for high-performance file copying with advanced features
#[cfg(target_os = "linux")]
#[derive(Deserialize)]
struct CpxKwargs {
    parallel: i32,
    dry_run: bool,
}

#[cfg(target_os = "linux")]
#[polars_expr(output_type=Boolean)]
fn cpx_file(inputs: &[Series], kwargs: CpxKwargs) -> PolarsResult<Series> {
    let from: &StringChunked = inputs[0].str()?;
    let to: &StringChunked = inputs[1].str()?;
    let parallel: i32 = kwargs.parallel;
    let dry_run: bool = kwargs.dry_run;
    let mut options = CopyOptions::none();
    options.parallel = parallel.max(0) as usize;
    let out: BooleanChunked =
        broadcast_binary_elementwise(from, to, |from: Option<&str>, to: Option<&str>| {
            match (from, to) {
                (Some(from), Some(to)) => {
                    let valid = check_file_to_file(Some(from), Some(to));
                    if !dry_run && valid {
                        copy(Path::new(from), Path::new(to), &options).is_ok()
                    } else {
                        valid
                    }
                },
                _ => false,
            }
        });
    Ok(out.into_series())
}
