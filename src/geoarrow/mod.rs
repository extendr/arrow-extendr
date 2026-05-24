//! Convert [geoarrow-array](https://docs.rs/geoarrow-array) structs to and from an `Robj`
//! via the Arrow C Data Interface.
//!
//! Gated behind the `geoarrow-08` feature flag.
//!
//! All types are exchanged as `nanoarrow_array` R external pointer objects, compatible
//! with the [{geoarrow}](https://geoarrow.github.io/geoarrow-r/) R package.
//!
//! ## Conversions
//!
//! | Type | `FromArrowRobj` | `ToArrowRobj` | `IntoArrowRobj` |
//! | ---- | :-------------: | :-----------: | :-------------: |
//! | `Arc<dyn GeoArrowArray>` | ✓ | ✓ | ✓ |
//! | `PointArray` | ✓ | ✓ | ✓ |
//! | `LineStringArray` | ✓ | ✓ | ✓ |
//! | `PolygonArray` | ✓ | ✓ | ✓ |
//! | `MultiPointArray` | ✓ | ✓ | ✓ |
//! | `MultiLineStringArray` | ✓ | ✓ | ✓ |
//! | `MultiPolygonArray` | ✓ | ✓ | ✓ |
//! | `GeometryArray` | ✓ | ✓ | ✓ |
//! | `GeometryCollectionArray` | ✓ | ✓ | ✓ |
//! | `RectArray` | ✓ | ✓ | ✓ |
//! | `WkbViewArray` | ✓ | ✓ | ✓ |
//! | `WktViewArray` | ✓ | ✓ | ✓ |
//!
//! ## Example
//!
//! ```ignore
//! use extendr_api::prelude::*;
//! use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
//! use geoarrow_array::array::PointArray;
//!
//! #[extendr]
//! /// @export
//! fn geoarrow_round_trip(x: Robj) -> extendr_api::Result<Robj> {
//!     let array = PointArray::from_arrow_robj(&x)
//!         .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
//!     array.into_arrow_robj()
//! }
//! ```
//!
//! ```r
//! library(geoarrow)
//! library(wk)
//!
//! pts <- as_geoarrow_array(xy(c(1, 2, 3), c(4, 5, 6)))
//! geoarrow_round_trip(pts)
//! ```

use std::sync::Arc;

use anyhow::{anyhow, bail};
use arrow::{
    array::{Array, make_array},
    datatypes::Field,
    ffi::{self, FFI_ArrowSchema},
};
use extendr_api::prelude::*;
use geoarrow_array::{
    GeoArrowArray,
    array::{
        GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray,
        MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray, WkbViewArray,
        WktViewArray, from_arrow_array,
    },
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

/// {geoarrow} R package has a `geoarrow-vctr` which is an integer vectors
/// with a list of `chunks` where each is its own geoarrow array
/// the `schema` is stored in the attribute `schema`
/// classes are: "geoarrow_vctr"  "nanoarrow_vctr"
pub struct GeoArrowVctr(Integers);

impl TryFrom<&Robj> for GeoArrowVctr {
    type Error = anyhow::Error;

    fn try_from(value: &Robj) -> Result<Self, Self::Error> {
        if !value.inherits("geoarrow_vctr") {
            bail!("Expected object of class `geoarrow_vctr`");
        }
        let Some(chunks) = value.get_attrib("chunks") else {
            bail!("`chunks` attribute missing from `geoarrow_vctr`");
        };

        let Some(_) = value.get_attrib("schema") else {
            bail!("`schema` attribute missing from `geoarrow_vctr`")
        };

        let Ok(_) = List::try_from(chunks) else {
            bail!("Expected `chunks` attribute to be a list");
        };

        let inner = Integers::try_from(value).map_err(|e| anyhow!("{e}"))?;
        Ok(Self(inner))
    }
}

impl GeoArrowVctr {
    fn chunks(&self) -> anyhow::Result<List> {
        let chunks = self
            .0
            .get_attrib("chunks")
            .ok_or_else(|| anyhow!("`chunks` attribute missing"))?;
        List::try_from(chunks).map_err(|e| anyhow!("{e}"))
    }

    fn schema(&self) -> anyhow::Result<Robj> {
        self.0
            .get_attrib("schema")
            .ok_or_else(|| anyhow!("`schema` attribute missing"))
    }

    fn iter_arrow(&self) -> anyhow::Result<Vec<(Arc<dyn Array>, Field)>> {
        let schema = self.schema()?;
        let ffi_schema = crate::nanoarrow::c_export_schema(&schema)?;
        let field = Field::try_from(ffi_schema)?;
        self.chunks()?
            .iter()
            .map(|(_, chunk)| {
                let ffi_array = crate::nanoarrow::c_export_array(&chunk)?;
                let array_data = unsafe { ffi::from_ffi(ffi_array, ffi_schema)? };
                Ok((make_array(array_data), field.clone()))
            })
            .collect()
    }

    pub fn as_point_chunks(&self) -> anyhow::Result<Vec<PointArray>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(PointArray::try_from((array.as_ref(), &field))?))
            .collect()
    }

    pub fn as_multipoint_chunks(&self) -> anyhow::Result<Vec<MultiPointArray>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(MultiPointArray::try_from((array.as_ref(), &field))?))
            .collect()
    }

    pub fn as_polygon_chunks(&self) -> anyhow::Result<Vec<PolygonArray>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(PolygonArray::try_from((array.as_ref(), &field))?))
            .collect()
    }

    pub fn as_multipolygon_chunks(&self) -> anyhow::Result<Vec<MultiPolygonArray>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(MultiPolygonArray::try_from((array.as_ref(), &field))?))
            .collect()
    }

    pub fn as_linestring_chunks(&self) -> anyhow::Result<Vec<LineStringArray>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(LineStringArray::try_from((array.as_ref(), &field))?))
            .collect()
    }

    pub fn as_multilinestring_chunks(&self) -> anyhow::Result<Vec<MultiLineStringArray>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(MultiLineStringArray::try_from((array.as_ref(), &field))?))
            .collect()
    }

    pub fn as_dyn_chunks(&self) -> anyhow::Result<Vec<Arc<dyn GeoArrowArray>>> {
        self.iter_arrow()?
            .into_iter()
            .map(|(array, field)| Ok(from_arrow_array(array.as_ref(), &field)?))
            .collect()
    }
}
