//! Convert arrow-rs structs into an `Robj`
//!
//! The traits `ToArrowRobj` and `IntoArrowRobj` provide the methods
//! `to_arrow_robj()` and `into_arrow_robj()` respectively. The former
//! takes a reference to self whereas the latter consumes self.
//!
//! Prefer `to_arrow_robj()` for all structs except `ArrowArrayStreamReader`.
//!
//! ```ignore
//! fn array_to_robj() -> Result<Robj> {
//!     let array = Int32Array::from(vec![Some(1), None, Some(3)]);
//!     array.to_arrow_robj()
//! }
//! ```
//!
//! |      arrow-rs struct     |         R object        |
//! | -------------------------| ----------------------- |
//! | `ArrayData`              |`nanoarrow_array`        |
//! | `PrimitiveArray<T>`      |`nanoarrow_array`        |
//! | `Field`                  |`nanoarrow_schema`       |
//! | `DataType`               |`nanoarrow_schema`       |
//! | `Schema`                 |`nanoarrow_schema`       |
//! | `RecordBatch`            |`nanoarrow_array_stream` |
//! | `ArrowArrayStreamReader` |`nanoarrow_array_stream` |
//!
use arrow::{
    array::{Array, ArrayData, PrimitiveArray},
    datatypes::{ArrowPrimitiveType, DataType, Field, Schema, SchemaBuilder},
    error::ArrowError,
    ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader},
};
use extendr_api::{error::Result, prelude::*};

use crate::{IntoArrowRobj, ToArrowRobj};

/// Calls `nanoarrow::nanoarrow_allocate_array()`
///
/// Requires `{nanoarrow}` to be installed.
pub fn allocate_array(args: Pairlist) -> Result<Robj> {
    R!("nanoarrow::nanoarrow_allocate_array")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_allocate_array()` must be available")
        .call(args)
}

/// Calls `nanoarrow::nanoarrow_allocate_array_stream()`
///
/// Requires `{nanoarrow}` to be installed.
pub fn allocate_array_stream(args: Pairlist) -> Result<Robj> {
    R!("nanoarrow::nanoarrow_allocate_array_stream")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_allocate_array()` must be available")
        .call(args)
}

/// Calls `nanoarrow::nanoarrow_allocate_schema()`
///
/// Requires `{nanoarrow}` to be installed.
pub fn allocate_schema(args: Pairlist) -> Result<Robj> {
    R!("nanoarrow::nanoarrow_allocate_schema")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_allocate_schema()` must be available")
        .call(args)
}

/// Calls `nanoarrow::nanoarrow_pointer_move()`
///
/// Requires `{nanoarrow}` to be installed.
pub fn move_pointer(args: Pairlist) -> Result<Robj> {
    R!("nanoarrow::nanoarrow_pointer_move")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_pointer_move()` must be available")
        .call(args)
}

/// Calls `nanoarrow::nanoarrow_array_set_schema()`
///
/// Requires `{nanoarrow}` to be installed.
pub fn set_array_schema(arr: &Robj, schema: &Robj) {
    let _ = R!("nanoarrow::nanoarrow_array_set_schema")
        .expect("`nanoarrow` must be installed")
        .as_function()
        .expect("`nanoarrow_array_set_schema()` must be available")
        .call(pairlist!(arr, schema));
}

impl ToArrowRobj for ArrayData {
    fn to_arrow_robj(&self) -> Result<Robj> {
        // take array data and prepare for FFI
        let (ffi_array, ffi_schema) = to_ffi(self).expect("success converting arrow data");

        // extract array pointer. we need it as a string to be used by arrow R package
        let ffi_array_ptr = &ffi_array as *const FFI_ArrowArray as usize;
        let arry_addr_chr = ffi_array_ptr.to_string();

        // same deal but with the schema
        let ffi_schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_schema_ptr.to_string();

        // allocate empty array and schema
        let arr_to_fill = allocate_array(pairlist!())?;
        let schema_to_fill = allocate_schema(pairlist!())?;

        // move pointers
        let _ = move_pointer(pairlist!(arry_addr_chr, &arr_to_fill));
        let _ = move_pointer(pairlist!(schema_addr_chr, &schema_to_fill));

        set_array_schema(&arr_to_fill, &schema_to_fill);

        Ok(arr_to_fill)
    }
}

impl<T: ArrowPrimitiveType> ToArrowRobj for PrimitiveArray<T> {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let data = self.into_data();
        data.to_arrow_robj()
    }
}

impl ToArrowRobj for Field {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let ffi_schema = FFI_ArrowSchema::try_from(self).expect("Field is FFI compatible");
        let ffi_schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_schema_ptr.to_string();

        // allocate the schema
        let schema_to_fill = allocate_schema(pairlist!())?;

        // fill the schema with the FFI_ArrowSchema
        let _ = move_pointer(pairlist!(schema_addr_chr, &schema_to_fill));

        Ok(schema_to_fill)
    }
}

impl ToArrowRobj for Schema {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let ffi_schema = FFI_ArrowSchema::try_from(self).expect("valid Schema");

        // allocate and get pntr address
        let ffi_schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_schema_ptr.to_string();

        // allocate the schema
        let schema_to_fill = allocate_schema(pairlist!())?;

        // fill the schema with the FFI_ArrowSchema
        let _ = move_pointer(pairlist!(schema_addr_chr, &schema_to_fill));

        Ok(schema_to_fill)
    }
}

impl ToArrowRobj for DataType {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let ffi_schema = FFI_ArrowSchema::try_from(self).expect("valid Schema");

        let ffi_schema_ptr = &ffi_schema as *const FFI_ArrowSchema as usize;
        let schema_addr_chr = ffi_schema_ptr.to_string();

        // allocate the schema
        let schema_to_fill = allocate_schema(pairlist!())?;

        // fill the schema with the FFI_ArrowSchema
        let _ = move_pointer(pairlist!(schema_addr_chr, &schema_to_fill));

        Ok(schema_to_fill)
    }
}

impl ToArrowRobj for RecordBatch {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let reader = RecordBatchIterator::new(vec![Ok(self.clone())], self.schema().clone());
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let mut stream = FFI_ArrowArrayStream::new(reader);
        let stream_ptr = (&mut stream) as *mut FFI_ArrowArrayStream as usize;

        let stream_to_fill = allocate_array_stream(pairlist!())?;
        let _ = move_pointer(pairlist!(stream_ptr.to_string(), &stream_to_fill));

        Ok(stream_to_fill)
    }
}

// macro to implement `IntoArrowRobj` for those that have `ToArrowRobj` implemented
macro_rules! impl_into_arrow {
    ($t:ident) => {
        impl IntoArrowRobj for $t {
            fn into_arrow_robj(self) -> Result<Robj> {
                self.to_arrow_robj()
            }
        }
    };
}

impl_into_arrow!(ArrayData);
impl_into_arrow!(Field);
impl_into_arrow!(Schema);
impl_into_arrow!(DataType);
impl_into_arrow!(RecordBatch);

// macro doesn't permit generics
impl<T: ArrowPrimitiveType> IntoArrowRobj for PrimitiveArray<T> {
    fn into_arrow_robj(self) -> Result<Robj> {
        self.to_arrow_robj()
    }
}

/// Function that will take an ArrowArrayStreamReader and turn into Robj
fn to_arrow_robj_stream_reader(reader: ArrowArrayStreamReader) -> Result<Robj> {
    let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
    let mut stream = FFI_ArrowArrayStream::new(reader);
    let stream_ptr = (&mut stream) as *mut FFI_ArrowArrayStream as usize;

    let stream_to_fill = allocate_array_stream(pairlist!())?;
    let _ = move_pointer(pairlist!(stream_ptr.to_string(), &stream_to_fill));

    Ok(stream_to_fill)
}

impl IntoArrowRobj for ArrowArrayStreamReader {
    fn into_arrow_robj(self) -> Result<Robj> {
        to_arrow_robj_stream_reader(self)
    }
}

impl IntoArrowRobj for Box<dyn RecordBatchReader + Send> {
    fn into_arrow_robj(self) -> Result<Robj> {
        let mut stream = FFI_ArrowArrayStream::new(self);
        let stream_ptr = (&mut stream) as *mut FFI_ArrowArrayStream as usize;

        let stream_to_fill = allocate_array_stream(pairlist!())?;
        let _ = move_pointer(pairlist!(stream_ptr.to_string(), &stream_to_fill));

        Ok(stream_to_fill)
    }
}

impl IntoArrowRobj for Vec<RecordBatch> {
    fn into_arrow_robj(self) -> Result<Robj> {
        // if there is an empty vector we create an empty RecordBatch
        if self.is_empty() {
            let sb = SchemaBuilder::new();
            let schema = sb.finish();
            let empty_iter = vec![].into_iter();
            let rb = arrow::record_batch::RecordBatchIterator::new(empty_iter, schema.into());
            return rb.into_arrow_robj();
        }

        let schema = self[0].schema();

        let res = self.into_iter().map(Ok::<RecordBatch, ArrowError>);

        let rbit = arrow::record_batch::RecordBatchIterator::new(res, schema);

        let reader: Box<dyn RecordBatchReader + Send> = Box::new(rbit);

        reader.into_arrow_robj()
    }
}

impl<I> IntoArrowRobj for RecordBatchIterator<I>
where
    I: IntoIterator<Item = std::result::Result<RecordBatch, ArrowError>> + Send + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    fn into_arrow_robj(self) -> Result<Robj> {
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(self);
        reader.into_arrow_robj()
    }
}
