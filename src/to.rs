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
    ffi::{FFI_ArrowSchema, to_ffi},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader},
};
use extendr_api::{error::Result, prelude::*};

use crate::{IntoArrowRobj, ToArrowRobj};

impl ToArrowRobj for ArrayData {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let (ffi_array, ffi_schema) = to_ffi(self).expect("success converting arrow data");
        let schema_robj = crate::nanoarrow::schema_to_robj(ffi_schema);
        Ok(crate::nanoarrow::array_to_robj(ffi_array, schema_robj))
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
        Ok(crate::nanoarrow::schema_to_robj(ffi_schema))
    }
}

impl ToArrowRobj for Schema {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let ffi_schema = FFI_ArrowSchema::try_from(self).expect("valid Schema");
        Ok(crate::nanoarrow::schema_to_robj(ffi_schema))
    }
}

impl ToArrowRobj for DataType {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let ffi_schema = FFI_ArrowSchema::try_from(self).expect("valid DataType");
        Ok(crate::nanoarrow::schema_to_robj(ffi_schema))
    }
}

impl ToArrowRobj for RecordBatch {
    fn to_arrow_robj(&self) -> Result<Robj> {
        let reader = RecordBatchIterator::new(vec![Ok(self.clone())], self.schema().clone());
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let stream = FFI_ArrowArrayStream::new(reader);
        let stream_to_fill = crate::nanoarrow::array_stream_to_robj(stream);
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
    let stream = FFI_ArrowArrayStream::new(Box::new(reader));
    Ok(crate::nanoarrow::array_stream_to_robj(stream))
}

impl IntoArrowRobj for ArrowArrayStreamReader {
    fn into_arrow_robj(self) -> Result<Robj> {
        to_arrow_robj_stream_reader(self)
    }
}

impl IntoArrowRobj for Box<dyn RecordBatchReader + Send> {
    fn into_arrow_robj(self) -> Result<Robj> {
        let stream = FFI_ArrowArrayStream::new(self);
        let stream_to_fill = crate::nanoarrow::array_stream_to_robj(stream);
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
