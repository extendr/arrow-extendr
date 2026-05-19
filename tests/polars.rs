#![cfg(any(feature = "polars-53", feature = "polars-51"))]

#[cfg(feature = "polars-51")]
extern crate polars_core_051 as polars_core;

use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::{Attributes, Error, R};
use extendr_engine::with_r;
use polars_core::frame::DataFrame;
use serial_test::serial;

#[test]
#[serial]
fn test_dataframe_from_r() -> anyhow::Result<()> {
    with_r(|| {
        let stream = R!(
            "nanoarrow::as_nanoarrow_array_stream(data.frame(a = 1:5, b = c(1.1, 2.2, 3.3, 4.4, 5.5)))"
        )?;
        let df =
            DataFrame::from_arrow_robj(&stream).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(df.height(), 5);
        assert_eq!(df.width(), 2);
        Ok::<(), Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_dataframe_into_r() -> anyhow::Result<()> {
    with_r(|| {
        let stream =
            R!("nanoarrow::as_nanoarrow_array_stream(data.frame(x = 1:10, y = rnorm(10)))")?;
        let df = DataFrame::from_arrow_robj(&stream).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(df.height(), 10);
        assert_eq!(df.width(), 2);
        let robj = df
            .into_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        assert!(robj.inherits("nanoarrow_array_stream"));
        Ok::<(), Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_dataframe_roundtrip() -> anyhow::Result<()> {
    with_r(|| {
        let stream = R!(
            "nanoarrow::as_nanoarrow_array_stream(data.frame(x = 1:5, y = c(1.1, 2.2, 3.3, 4.4, 5.5)))"
        )?;
        let df =
            DataFrame::from_arrow_robj(&stream).map_err(|e| Error::Other(e.to_string()))?;
        let robj = df.into_arrow_robj().map_err(|e| Error::Other(e.to_string()))?;
        let result = R!("as.data.frame(nanoarrow::collect_array_stream({{robj}}))")?;
        let nrow = R!("nrow({{result.clone()}})")?.as_integer().unwrap();
        let ncol = R!("ncol({{result}})")?.as_integer().unwrap();
        assert_eq!(nrow, 5);
        assert_eq!(ncol, 2);
        Ok::<(), Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_dataframe_column_names_preserved() -> anyhow::Result<()> {
    with_r(|| {
        let stream = R!(
            "nanoarrow::as_nanoarrow_array_stream(data.frame(foo = 1:3, bar = c(1.0, 2.0, 3.0)))"
        )?;
        let df = DataFrame::from_arrow_robj(&stream).map_err(|e| Error::Other(e.to_string()))?;
        let names = df
            .get_column_names()
            .into_iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names, vec!["foo", "bar"]);
        Ok::<(), Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_empty_dataframe_roundtrip() -> anyhow::Result<()> {
    with_r(|| {
        let stream =
            R!("nanoarrow::as_nanoarrow_array_stream(data.frame(x = integer(0), y = double(0)))")?;
        let df = DataFrame::from_arrow_robj(&stream).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(df.height(), 0);
        assert_eq!(df.width(), 2);
        let robj = df
            .into_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        assert!(robj.inherits("nanoarrow_array_stream"));
        Ok::<(), Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
