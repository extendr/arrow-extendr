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

/// The WKT of every row the chunks hold, laid end to end, as R sees it.
fn chunks_wkt(chunks: Vec<PointArray>) -> extendr_api::Result<extendr_api::Robj> {
    let parts = chunks
        .into_iter()
        .map(|chunk| chunk.into_arrow_robj())
        .collect::<extendr_api::Result<Vec<_>>>()?;
    let list = extendr_api::prelude::List::from_values(parts);
    R!("unlist(lapply({{list}}, function(a) geoarrow::geoarrow_handle(a, wk::wkt_writer())))")
}

fn point_chunks_of(r_expr: &str) -> extendr_api::Result<Vec<PointArray>> {
    let vctr = extendr_api::eval_string(r_expr)?;
    arrow_extendr::geoarrow::GeoArrowVctr::try_from(&vctr)
        .and_then(|v| v.as_point_chunks())
        .map_err(|e| extendr_api::Error::Other(e.to_string()))
}

fn assert_wkt(r_expr: &str, expected: &str) -> anyhow::Result<()> {
    with_r(|| {
        let chunks = point_chunks_of(r_expr)?;
        let got = chunks_wkt(chunks)?;
        let report = format!("{got:?}");
        let expected_robj = extendr_api::eval_string(expected)?;
        let same = R!("identical({{got}}, {{expected_robj}})")?;
        assert!(
            same.as_logical().map(|v| v.is_true()).unwrap_or(false),
            "{r_expr} gave {report}, expected {expected}"
        );
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

const PTS: &str = "geoarrow::as_geoarrow_vctr(wk::xy(1:5, 6:10))";

#[test]
#[serial]
fn test_geoarrow_vctr_reads_every_row() -> anyhow::Result<()> {
    assert_wkt(
        PTS,
        "c('POINT (1 6)', 'POINT (2 7)', 'POINT (3 8)', 'POINT (4 9)', 'POINT (5 10)')",
    )
}

#[test]
#[serial]
fn test_geoarrow_vctr_respects_a_contiguous_slice() -> anyhow::Result<()> {
    assert_wkt(
        &format!("{PTS}[2:4]"),
        "c('POINT (2 7)', 'POINT (3 8)', 'POINT (4 9)')",
    )
}

#[test]
#[serial]
fn test_geoarrow_vctr_respects_a_single_row() -> anyhow::Result<()> {
    assert_wkt(&format!("{PTS}[4]"), "'POINT (4 9)'")
}

#[test]
#[serial]
fn test_geoarrow_vctr_respects_a_reordering() -> anyhow::Result<()> {
    assert_wkt(
        &format!("{PTS}[c(5L, 1L, 3L)]"),
        "c('POINT (5 10)', 'POINT (1 6)', 'POINT (3 8)')",
    )
}

#[test]
#[serial]
fn test_geoarrow_vctr_respects_a_repeat() -> anyhow::Result<()> {
    assert_wkt(
        &format!("{PTS}[c(2L, 2L)]"),
        "c('POINT (2 7)', 'POINT (2 7)')",
    )
}

#[test]
#[serial]
fn test_geoarrow_vctr_reads_na_as_a_null() -> anyhow::Result<()> {
    assert_wkt(&format!("{PTS}[NA_integer_]"), "NA_character_")
}

#[test]
#[serial]
fn test_geoarrow_vctr_reads_an_empty_selection() -> anyhow::Result<()> {
    with_r(|| {
        let chunks = point_chunks_of(&format!("{PTS}[0]"))?;
        assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 0);
        // an empty selection still carries the type, so callers can read it
        assert_eq!(chunks.len(), 1);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_geoarrow_vctr_slices_across_chunks() -> anyhow::Result<()> {
    let two = format!("vctrs::vec_c({PTS}, {PTS})");
    assert_wkt(
        &format!("{two}[4:7]"),
        "c('POINT (4 9)', 'POINT (5 10)', 'POINT (1 6)', 'POINT (2 7)')",
    )
}

#[test]
#[serial]
fn test_geoarrow_vctr_reads_empty_extension_metadata() -> anyhow::Result<()> {
    // an array with no metadata gains the key with an empty value on a round trip
    assert_wkt(
        "geoarrow::as_geoarrow_vctr(nanoarrow::nanoarrow_extension_array(
           nanoarrow::as_nanoarrow_array(data.frame(x = c(1, 2, 3), y = c(4, 5, 6))),
           'geoarrow.point'
         ))[c(3L, 1L)]",
        "c('POINT (3 6)', 'POINT (1 4)')",
    )
}
