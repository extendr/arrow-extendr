//! Create arrow-rs objects from an `Robj`
//!
//! ```ignore
//! fn array_from_r(field: Robj) -> Result<ArrayData, anyhow::Error> {
//!     ArrayData::from_arrow_robj(&field)
//! }
//! ```
//!
//! `Robj`s from `{nanoarrow}` and `{arrow}` are both supported.
//!
//! | arrow-rs struct          |             R object                            |
//! | -------------------------| ----------------------------------------------- |
//! | `Field`                  |`nanoarrow_schema` or `arrow::Field`             |
//! | `Schema`                 |`nanoarrow_schema` or `arrow::Schema`            |
//! | `DataType`               |`nanoarrow_schema` or `arrow::DataType`          |
//! | `ArrayData`              |`nanoarrow_array` or `arrow::Array`              |
//! | `RecordBatch`            |`nanoarrow_array_stream` or `arrow::RecordBatch` |
//! | `ArrowArrayStreamReader` |`nanoarrow_array_stream`                         |
//!
//! ### Notes
//!
//! In the case of creating a `RecordBatch` from a `nanoarrow_array_stream` only
//! the first chunk is returned. If you expect more than one chunk, use `ArrowArrayStreamReader`.
//!

use crate::FromArrowRobj;
use anyhow::Result;
use arrow::{
    array::{make_array, ArrayData},
    datatypes::{DataType, Field, Schema},
    ffi::{self, FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{self, ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::RecordBatch,
};
use extendr_api::prelude::*;

/// Calls `nanoarrow::nanoarrow_pointer_addr_chr()`
///
/// Gets the address of a nanoarrow object as a string `Robj`
/// Requires `{nanoarrow}` to be installed.
pub fn nanoarrow_addr(robj: &Robj) -> Result<Robj, Error> {
    R!("nanoarrow::nanoarrow_pointer_addr_chr")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_pointer_addr_ch()` must be available")
        .call(pairlist!(robj))
}

/// Calls `nanoarrow::nanoarrow_pointer_export()`
///
/// Exports a nanoarrow pointer from R to C
/// Requires `{nanoarrow}` to be installed.
pub fn nanoarrow_export(source: &Robj, dest: String) -> Result<Robj, Error> {
    R!("nanoarrow::nanoarrow_pointer_export")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_pointer_export()` must be available")
        .call(pairlist!(source, dest))
}

impl FromArrowRobj for Field {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if robj.inherits("nanoarrow_schema") {
            let c_schema = FFI_ArrowSchema::empty();
            let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;
            let _ = nanoarrow_export(robj, c_schema_ptr.to_string());
            return Ok(Field::try_from(&c_schema)?);
        }

        if !robj.inherits("Field") {
            return Err(anyhow::anyhow!(
                "did not find a `Field` or `nanoarrow_schema`"
            ));
        }

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;
        let _ = export_to_c.call(pairlist!(c_schema_ptr.to_string()));
        Ok(Field::try_from(&c_schema)?)
    }
}

impl FromArrowRobj for DataType {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if robj.inherits("nanoarrow_schema") {
            let c_schema = FFI_ArrowSchema::empty();
            let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;
            let _ = nanoarrow_export(robj, c_schema_ptr.to_string());
            return Ok(DataType::try_from(&c_schema)?);
        }

        if !robj.inherits("DataType") {
            return Err(anyhow::anyhow!(
                "did not find a `DataType` or `nanoarrow_schema`"
            ));
        }

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;
        let _ = export_to_c.call(pairlist!(c_schema_ptr.to_string()));
        Ok(DataType::try_from(&c_schema)?)
    }
}

impl FromArrowRobj for Schema {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if robj.inherits("nanoarrow_schema") {
            let c_schema = FFI_ArrowSchema::empty();
            let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;
            let _ = nanoarrow_export(robj, c_schema_ptr.to_string());
            return Ok(Schema::try_from(&c_schema)?);
        }

        if !robj.inherits("Schema") {
            return Err(anyhow::anyhow!(
                "did not find a `Schema` or `nanoarrow_schema`"
            ));
        }

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema as usize;
        let _ = export_to_c.call(pairlist!(c_schema_ptr.to_string()));
        Ok(Schema::try_from(&c_schema)?)
    }
}

// https://github.com/apache/arrow-rs/blob/200e8c80084442d9579e00967e407cd83191565d/arrow/src/pyarrow.rs#L248
impl FromArrowRobj for ArrayData {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if robj.inherits("nanoarrow_array") {
            let array = FFI_ArrowArray::empty();
            let schema = FFI_ArrowSchema::empty();

            let c_array_ptr = &array as *const FFI_ArrowArray as usize;
            let c_schema_ptr = &schema as *const FFI_ArrowSchema as usize;

            let robj_schema = R!("nanoarrow::infer_nanoarrow_schema")
                .unwrap()
                .as_function()
                .unwrap()
                .call(pairlist!(robj))
                .expect("unable to infer nanoarrow schema");

            let _ = nanoarrow_export(robj, c_array_ptr.to_string());
            let _ = nanoarrow_export(&robj_schema, c_schema_ptr.to_string());

            return Ok(unsafe { ffi::from_ffi(array, &schema)? });
        }

        if !robj.inherits("Array") {
            return Err(anyhow::anyhow!("did not find a `Array`"));
        }

        let array = FFI_ArrowArray::empty();
        let schema = FFI_ArrowSchema::empty();

        let c_array_ptr = &array as *const FFI_ArrowArray as usize;
        let c_schema_ptr = &schema as *const FFI_ArrowSchema as usize;

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let _ = export_to_c.call(pairlist!(c_array_ptr.to_string(), c_schema_ptr.to_string()));

        Ok(unsafe { ffi::from_ffi(array, &schema)? })
    }
}

/// If there are more than one RecordBatches in the stream, do not use this.
/// Use `ArrowArrayStreamReader` instead.
impl FromArrowRobj for RecordBatch {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if robj.inherits("nanoarrow_array_stream") {
            let stream = ffi_stream::FFI_ArrowArrayStream::empty();
            let c_stream_ptr = &stream as *const FFI_ArrowArrayStream as usize;
            let _ = nanoarrow_export(robj, c_stream_ptr.to_string());
            let res = ArrowArrayStreamReader::try_new(stream)?;
            let r2 = res.into_iter().map(|xi| xi.unwrap()).nth(0).unwrap();
            return Ok(r2);
        }

        if !robj.inherits("RecordBatch") {
            return Err(anyhow::anyhow!(
                "did not find a `RecordBatch` or `nanoarrow_array_stream`"
            ));
        }

        let array = FFI_ArrowArray::empty();
        let schema = FFI_ArrowSchema::empty();

        let c_array_ptr = &array as *const FFI_ArrowArray as usize;
        let c_schema_ptr = &schema as *const FFI_ArrowSchema as usize;

        let export_to_c = robj
            .dollar("export_to_c")
            .expect("export_to_c() method to be available")
            .as_function()
            .unwrap();

        let _ = export_to_c.call(pairlist!(c_array_ptr.to_string(), c_schema_ptr.to_string()));

        let res = unsafe { ffi::from_ffi(array, &schema)? };
        let schema = Schema::try_from(&schema)?;

        let res_arrays = res
            .child_data()
            .iter()
            .map(|xi| make_array(xi.clone()))
            .collect::<Vec<_>>();

        Ok(RecordBatch::try_new(schema.into(), res_arrays)?)
    }
}

impl FromArrowRobj for ArrowArrayStreamReader {
    fn from_arrow_robj(robj: &Robj) -> std::result::Result<Self, anyhow::Error> {
        if !robj.inherits("nanoarrow_array_stream") {
            return Err(anyhow::anyhow!("did not find `nanoarrow_array_stream`"));
        }
        let stream = ffi_stream::FFI_ArrowArrayStream::empty();
        let c_stream_ptr = &stream as *const FFI_ArrowArrayStream as usize;
        let _ = nanoarrow_export(robj, c_stream_ptr.to_string());
        Ok(ArrowArrayStreamReader::try_new(stream)?)
    }
}
