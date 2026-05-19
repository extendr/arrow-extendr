use arrow_extendr::{FromArrowRobj, IntoArrowRobj};
use extendr_api::R;
use extendr_engine::with_r;
use geoarrow_array::{
    GeoArrowArray,
    array::{
        GeometryArray, GeometryCollectionArray, LineStringArray, MultiLineStringArray,
        MultiPointArray, MultiPolygonArray, PointArray, PolygonArray, RectArray, WkbViewArray,
        WktViewArray,
    },
    builder::{GeometryBuilder, GeometryCollectionBuilder, RectBuilder},
};
use geoarrow_schema::{BoxType, Dimension, GeometryCollectionType, GeometryType, Metadata};
use serial_test::serial;
use std::sync::Arc;

fn wkt_of(arr: &extendr_api::Robj) -> extendr_api::Robj {
    R!("geoarrow::geoarrow_handle({{arr}}, wk::wkt_writer())").unwrap()
}

fn wkt_roundtrip_ok(original_wkt: &extendr_api::Robj, roundtrip: &extendr_api::Robj) -> bool {
    let roundtrip_wkt = wkt_of(roundtrip);
    R!("identical({{original_wkt}}, {{roundtrip_wkt}})")
        .unwrap()
        .as_logical()
        .unwrap()
        .is_true()
}

macro_rules! roundtrip_test {
    ($name:ident, $ty:ty, $r_expr:expr) => {
        #[test]
        #[serial]
        fn $name() -> anyhow::Result<()> {
            with_r(|| {
                let original = R!($r_expr)?;
                // Extract WKT before from_arrow_robj consumes the nanoarrow_array.
                let original_wkt = wkt_of(&original);
                let array = <$ty>::from_arrow_robj(&original)
                    .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
                let robj = array.into_arrow_robj()?;
                assert!(wkt_roundtrip_ok(&original_wkt, &robj));
                Ok::<(), extendr_api::Error>(())
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?;
            Ok(())
        }
    };
}

#[test]
#[serial]
fn test_geoarrow_point_from_r() -> anyhow::Result<()> {
    with_r(|| {
        let x =
            R!("geoarrow::as_geoarrow_array(wk::xy(rnorm(100, -180, 180), runif(100, -90, 90)))")?;
        let array = Arc::<dyn GeoArrowArray>::from_arrow_robj(&x)
            .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
        assert_eq!(array.len(), 100);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

roundtrip_test!(
    test_geoarrow_point_roundtrip,
    PointArray,
    "geoarrow::as_geoarrow_array(wk::xy(c(1.0, 2.0, 3.0), c(4.0, 5.0, 6.0)))"
);

roundtrip_test!(
    test_geoarrow_linestring_roundtrip,
    LineStringArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkt(c("LINESTRING (0 0, 1 1, 2 2)", "LINESTRING (3 3, 4 4)")),
        schema = geoarrow::geoarrow_linestring()
    )"#
);

roundtrip_test!(
    test_geoarrow_polygon_roundtrip,
    PolygonArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkt(c("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))", "POLYGON ((2 2, 3 2, 3 3, 2 3, 2 2))")),
        schema = geoarrow::geoarrow_polygon()
    )"#
);

roundtrip_test!(
    test_geoarrow_multipoint_roundtrip,
    MultiPointArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkt(c("MULTIPOINT ((0 0), (1 1))", "MULTIPOINT ((2 2), (3 3))")),
        schema = geoarrow::geoarrow_multipoint()
    )"#
);

roundtrip_test!(
    test_geoarrow_multilinestring_roundtrip,
    MultiLineStringArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkt(c("MULTILINESTRING ((0 0, 1 1), (2 2, 3 3))")),
        schema = geoarrow::geoarrow_multilinestring()
    )"#
);

roundtrip_test!(
    test_geoarrow_multipolygon_roundtrip,
    MultiPolygonArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkt(c("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 1, 0 0)))")),
        schema = geoarrow::geoarrow_multipolygon()
    )"#
);

roundtrip_test!(
    test_geoarrow_wkb_view_roundtrip,
    WkbViewArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkb(wk::as_wkb(wk::xy(c(1.0, 2.0), c(3.0, 4.0)))),
        schema = geoarrow::geoarrow_wkb_view()
    )"#
);

roundtrip_test!(
    test_geoarrow_wkt_view_roundtrip,
    WktViewArray,
    r#"geoarrow::as_geoarrow_array(
        wk::wkt(wk::as_wkt(wk::xy(c(1.0, 2.0), c(3.0, 4.0)))),
        schema = geoarrow::geoarrow_wkt_view()
    )"#
);

#[test]
#[serial]
fn test_geoarrow_geometry_roundtrip() -> anyhow::Result<()> {
    use geo_types::{Coord, Geometry, LineString, Point};
    with_r(|| {
        let geoms: Vec<Option<Geometry>> = vec![
            Some(Point::new(0.0_f64, 1.0).into()),
            Some(LineString(vec![Coord { x: 0.0_f64, y: 0.0 }, Coord { x: 1.0, y: 1.0 }]).into()),
        ];
        let typ = GeometryType::new(Arc::new(Metadata::default()));
        let array = GeometryBuilder::from_nullable_geometries(&geoms, typ)
            .map_err(|e| extendr_api::Error::Other(e.to_string()))?
            .finish();
        let len = array.len();
        let robj = array.into_arrow_robj()?;
        let roundtrip = GeometryArray::from_arrow_robj(&robj)
            .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
        assert_eq!(roundtrip.len(), len);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_geoarrow_rect_roundtrip() -> anyhow::Result<()> {
    use geo_types::{Coord, Rect};
    with_r(|| {
        let rects = [
            Rect::new(Coord { x: 0.0_f64, y: 0.0 }, Coord { x: 1.0_f64, y: 1.0 }),
            Rect::new(Coord { x: 2.0_f64, y: 2.0 }, Coord { x: 3.0_f64, y: 3.0 }),
        ];
        let typ = BoxType::new(Dimension::XY, Arc::new(Metadata::default()));
        let array = RectBuilder::from_rects(rects.iter(), typ).finish();
        let len = array.len();
        let robj = array.into_arrow_robj()?;
        let roundtrip = RectArray::from_arrow_robj(&robj)
            .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
        assert_eq!(roundtrip.len(), len);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_geoarrow_geometry_collection_roundtrip() -> anyhow::Result<()> {
    use geo_types::{Coord, Geometry, GeometryCollection, LineString, Point};
    with_r(|| {
        let collections: Vec<GeometryCollection> = vec![
            GeometryCollection(vec![Geometry::Point(Point::new(0.0_f64, 1.0))]),
            GeometryCollection(vec![Geometry::LineString(LineString(vec![
                Coord { x: 0.0_f64, y: 0.0 },
                Coord { x: 1.0, y: 1.0 },
            ]))]),
        ];
        let typ = GeometryCollectionType::new(Dimension::XY, Arc::new(Metadata::default()));
        let array = GeometryCollectionBuilder::from_geometry_collections(&collections, typ)
            .map_err(|e| extendr_api::Error::Other(e.to_string()))?
            .finish();
        let len = array.len();
        let robj = array.into_arrow_robj()?;
        let roundtrip = GeometryCollectionArray::from_arrow_robj(&robj)
            .map_err(|e| extendr_api::Error::Other(e.to_string()))?;
        assert_eq!(roundtrip.len(), len);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
