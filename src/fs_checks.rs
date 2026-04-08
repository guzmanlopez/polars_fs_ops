#![allow(clippy::unused_unit)]
use std::path::Path;

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

use crate::utils::valid_parent_dir;

#[polars_expr(output_type=Boolean)]
fn file_exists(inputs: &[Series]) -> PolarsResult<Series> {
    let fp: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked = fp
        .apply_nonnull_values_generic(DataType::Boolean, |value: &str| Path::new(value).is_file());

    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn check_valid_parent_dir(inputs: &[Series]) -> PolarsResult<Series> {
    let fp: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked = fp.apply_nonnull_values_generic(DataType::Boolean, |value: &str| {
        valid_parent_dir(Some(value))
    });

    Ok(out.into_series())
}
