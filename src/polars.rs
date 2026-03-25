//! Convert polars-arrow structs to and from an `Robj` via the C stream interface.
//!
//! Gated behind the `polars` feature flag.
//!
//! | polars-arrow type              | R object                |
//! | ------------------------------ | ----------------------- |
//! | `ArrowArrayStreamReader`       | `nanoarrow_array_stream`|
//! | `ArrowArrayStream`             | `nanoarrow_array_stream`|
//! | `DataFrame`                    | `nanoarrow_array_stream`|
//! | `DataFrame` (from R)           | `nanoarrow_array_stream`|

use crate::{
    from::nanoarrow_export,
    to::{allocate_array_stream, move_pointer},
    FromArrowRobj, IntoArrowRobj,
};
use extendr_api::{error::Result, prelude::*};
use polars_core::utils::arrow::{
    array::{Array, StructArray},
    datatypes::{ArrowDataType, Field},
    ffi::{self, ArrowArrayStream, ArrowArrayStreamReader},
    legacy::error::PolarsResult,
};
use polars_core::{
    frame::DataFrame,
    prelude::{Column, CompatLevel},
    schema::SchemaExt,
    series::Series,
};

// ── From R ───────────────────────────────────────────────────────────────────

impl FromArrowRobj for ArrowArrayStreamReader<Box<ArrowArrayStream>> {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if !robj.inherits("nanoarrow_array_stream") {
            return Err(anyhow::anyhow!("expected a `nanoarrow_array_stream`"));
        }

        let mut stream = Box::new(ArrowArrayStream::empty());
        let stream_ptr = stream.as_mut() as *mut ArrowArrayStream as usize;

        let _ =
            nanoarrow_export(robj, stream_ptr.to_string()).map_err(|e| anyhow::anyhow!("{e}"))?;

        // SAFETY: the pointer was just populated by nanoarrow_pointer_export
        unsafe { ArrowArrayStreamReader::try_new(stream).map_err(|e| anyhow::anyhow!("{e}")) }
    }
}

impl FromArrowRobj for DataFrame {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        let mut reader = ArrowArrayStreamReader::<Box<ArrowArrayStream>>::from_arrow_robj(robj)?;

        let field = reader.field().clone();

        let mut chunks: Vec<StructArray> = Vec::new();

        while let Some(arr) = unsafe { reader.next() } {
            let arr = arr.map_err(|e| anyhow::anyhow!("{e}"))?;
            let struct_arr = arr
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| anyhow::anyhow!("expected a StructArray batch"))?
                .clone();
            chunks.push(struct_arr);
        }

        if chunks.is_empty() {
            return Ok(DataFrame::empty());
        }

        let fields = match &field.dtype {
            ArrowDataType::Struct(fields) => fields.clone(),
            _ => return Err(anyhow::anyhow!("stream schema must be a struct type")),
        };

        let height = chunks.first().map_or(0, |c| c.len());

        let columns = fields
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let arrays = chunks
                    .iter()
                    .map(|chunk| chunk.values()[i].clone())
                    .collect::<Vec<_>>();
                Series::try_from((f, arrays))
                    .map(Column::from)
                    .map_err(|e| anyhow::anyhow!("{e}"))
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        DataFrame::new(height, columns).map_err(|e| anyhow::anyhow!("{e}"))
    }
}

// ── To R ─────────────────────────────────────────────────────────────────────

impl IntoArrowRobj for ArrowArrayStream {
    fn into_arrow_robj(mut self) -> Result<Robj> {
        let stream_ptr = (&mut self) as *mut ArrowArrayStream as usize;

        let stream_to_fill = allocate_array_stream(pairlist!())?;
        let _ = move_pointer(pairlist!(stream_ptr.to_string(), &stream_to_fill));

        Ok(stream_to_fill)
    }
}

impl IntoArrowRobj for DataFrame {
    fn into_arrow_robj(self) -> Result<Robj> {
        let compat_level = CompatLevel::newest();
        let schema = self.schema();
        let fields = schema
            .iter_fields()
            .map(|xi| xi.to_arrow(compat_level))
            .collect::<Vec<_>>();
        let dtype = ArrowDataType::Struct(fields);
        let schema_field = Field::new("".into(), dtype.clone(), false);

        let columns = self.columns().to_vec();
        let n_chunks = columns.first().map_or(0, |s| s.n_chunks());

        let iter: Box<dyn Iterator<Item = PolarsResult<Box<dyn Array>>>> =
            Box::new((0..n_chunks).map(move |chunk_idx| {
                let arrays = columns
                    .iter()
                    .map(|s| {
                        s.clone()
                            .into_materialized_series()
                            .to_arrow(chunk_idx, compat_level)
                    })
                    .collect::<Vec<_>>();
                let length = arrays.first().map_or(0, |a| a.len());
                let arr = StructArray::new(dtype.clone(), length, arrays, None);
                Ok(Box::new(arr) as Box<dyn Array>)
            }));

        ffi::export_iterator(iter, schema_field).into_arrow_robj()
    }
}
