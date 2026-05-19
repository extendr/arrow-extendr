use std::sync::Arc;

use arrow::{
    array::{make_array, Array},
    datatypes::Field,
    ffi::{self, FFI_ArrowSchema},
};
use extendr_api::prelude::*;
use geoarrow_array::{
    array::{
        from_arrow_array, GeometryArray, GeometryCollectionArray, LineStringArray,
        MultiLineStringArray, MultiPointArray, MultiPolygonArray, PointArray, PolygonArray,
        RectArray, WkbViewArray, WktViewArray,
    },
    GeoArrowArray,
};

use crate::{FromArrowRobj, IntoArrowRobj, ToArrowRobj};

/// Shared extraction of (`Arc<dyn Array>`, `Field`) from a `nanoarrow_array` Robj.
fn nanoarrow_to_arrow(robj: &Robj) -> anyhow::Result<(Arc<dyn Array>, Field)> {
    if !robj.inherits("nanoarrow_array") {
        return Err(anyhow::anyhow!("expected a `nanoarrow_array`"));
    }
    let schema_robj = crate::nanoarrow::infer_schema_array(robj.clone())?;
    let ffi_schema = crate::nanoarrow::c_export_schema(&schema_robj)?;
    let field = Field::try_from(ffi_schema)?;
    let ffi_array = crate::nanoarrow::c_export_array(robj)?;
    let array_data = unsafe { ffi::from_ffi(ffi_array, ffi_schema)? };
    Ok((make_array(array_data), field))
}

impl FromArrowRobj for Arc<dyn GeoArrowArray> {
    fn from_arrow_robj(robj: &Robj) -> anyhow::Result<Self> {
        let (array, field) = nanoarrow_to_arrow(robj)?;
        Ok(from_arrow_array(array.as_ref(), &field)?)
    }
}

macro_rules! impl_from_arrow_robj_geoarrow {
    ($($ty:ty),* $(,)?) => {
        $(
            impl FromArrowRobj for $ty {
                fn from_arrow_robj(robj: &Robj) -> anyhow::Result<Self> {
                    let (array, field) = nanoarrow_to_arrow(robj)?;
                    Ok(Self::try_from((array.as_ref(), &field))?)
                }
            }
        )*
    };
}

impl_from_arrow_robj_geoarrow!(
    PointArray,
    LineStringArray,
    PolygonArray,
    MultiPointArray,
    MultiLineStringArray,
    MultiPolygonArray,
    GeometryArray,
    GeometryCollectionArray,
    RectArray,
    WkbViewArray,
    WktViewArray,
);

/// Shared conversion of a `GeoArrowArray` to a `nanoarrow_array` Robj.
fn geoarrow_to_nanoarrow(array: &dyn GeoArrowArray) -> extendr_api::Result<Robj> {
    let field = array.data_type().to_field("", true);
    let ffi_schema =
        FFI_ArrowSchema::try_from(&field).map_err(|e| extendr_api::Error::Other(e.to_string()))?;
    let array_ref = array.to_array_ref();
    let (ffi_array, _) =
        ffi::to_ffi(&array_ref.to_data()).map_err(|e| extendr_api::Error::Other(e.to_string()))?;
    let schema_robj = crate::nanoarrow::schema_to_robj(ffi_schema);
    Ok(crate::nanoarrow::array_to_robj(ffi_array, schema_robj))
}

impl ToArrowRobj for Arc<dyn GeoArrowArray> {
    fn to_arrow_robj(&self) -> extendr_api::Result<Robj> {
        geoarrow_to_nanoarrow(self.as_ref())
    }
}

impl IntoArrowRobj for Arc<dyn GeoArrowArray> {
    fn into_arrow_robj(self) -> extendr_api::Result<Robj> {
        self.to_arrow_robj()
    }
}

macro_rules! impl_to_arrow_robj_geoarrow {
    ($($ty:ty),* $(,)?) => {
        $(
            impl ToArrowRobj for $ty {
                fn to_arrow_robj(&self) -> extendr_api::Result<Robj> {
                    geoarrow_to_nanoarrow(self)
                }
            }

            impl IntoArrowRobj for $ty {
                fn into_arrow_robj(self) -> extendr_api::Result<Robj> {
                    self.to_arrow_robj()
                }
            }
        )*
    };
}

impl_to_arrow_robj_geoarrow!(
    PointArray,
    LineStringArray,
    PolygonArray,
    MultiPointArray,
    MultiLineStringArray,
    MultiPolygonArray,
    GeometryArray,
    GeometryCollectionArray,
    RectArray,
    WkbViewArray,
    WktViewArray,
);
