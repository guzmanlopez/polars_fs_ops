#![allow(clippy::unused_unit)]

use polars::prelude::*;
use pyo3_polars::derive::polars_expr;

use crate::utils::{has_valid_parent_dir, is_valid_source};

#[polars_expr(output_type=Boolean)]
fn file_exists(inputs: &[Series]) -> PolarsResult<Series> {
    let fp: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked = fp.apply_nonnull_values_generic(DataType::Boolean, |value: &str| {
        is_valid_source(Some(value))
    });

    Ok(out.into_series())
}

#[polars_expr(output_type=Boolean)]
fn check_valid_parent_dir(inputs: &[Series]) -> PolarsResult<Series> {
    let fp: &StringChunked = inputs[0].str()?;
    let out: BooleanChunked = fp.apply_nonnull_values_generic(DataType::Boolean, |value: &str| {
        has_valid_parent_dir(Some(value))
    });

    Ok(out.into_series())
}
