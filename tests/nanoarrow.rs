use arrow::{
    array::{Array, ArrayData, Int32Array},
    datatypes::{DataType, Field, Schema},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader},
};
use arrow_extendr::{FromArrowRobj, IntoArrowRobj, ToArrowRobj};
use extendr_api::{Error, R};
use extendr_engine::with_r;
use serial_test::serial;
use std::sync::Arc;

#[test]
#[serial]
fn test_roundtrip_record_batch() -> anyhow::Result<()> {
    with_r(|| {
        let stream = R!("nanoarrow::as_nanoarrow_array_stream(penguins)")?;
        let rb = RecordBatch::from_arrow_robj(&stream).map_err(|e| Error::Other(e.to_string()))?;
        let robj = rb
            .into_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        let identical = R!("all.equal(
            as.data.frame(nanoarrow::as_nanoarrow_array_stream({{robj}})),
            as.data.frame(lapply(penguins, function(x) if (is.factor(x)) as.character(x) else x))
        )")?;
        assert!(identical.as_logical().unwrap().is_true());
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_roundtrip_field() -> anyhow::Result<()> {
    with_r(|| {
        let field = Field::new("x", DataType::Int32, true);
        let robj = field
            .to_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        let field2 = Field::from_arrow_robj(&robj).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(field, field2);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_roundtrip_schema() -> anyhow::Result<()> {
    with_r(|| {
        let schema = Schema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Float64, false),
        ]);
        let robj = schema
            .to_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        let schema2 = Schema::from_arrow_robj(&robj).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(schema, schema2);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_roundtrip_data_type() -> anyhow::Result<()> {
    with_r(|| {
        let dt = DataType::Int32;
        let robj = dt
            .to_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        let dt2 = DataType::from_arrow_robj(&robj).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(dt, dt2);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_roundtrip_array_data() -> anyhow::Result<()> {
    with_r(|| {
        let array = Int32Array::from(vec![Some(1), None, Some(3)]);
        let data = array.into_data();
        let robj = data
            .to_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;
        let data2 = ArrayData::from_arrow_robj(&robj).map_err(|e| Error::Other(e.to_string()))?;
        assert_eq!(data, data2);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_roundtrip_vec_record_batch() -> anyhow::Result<()> {
    with_r(|| {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
        )
        .map_err(|e| Error::Other(e.to_string()))?;
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(4), None, Some(6)]))],
        )
        .map_err(|e| Error::Other(e.to_string()))?;

        let robj = vec![batch1.clone(), batch2.clone()]
            .into_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;

        let result = ArrowArrayStreamReader::from_arrow_robj(&robj)
            .map_err(|e| Error::Other(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Error::Other(e.to_string()))?;

        assert_eq!(result.len(), 2);
        assert_eq!(result[0], batch1);
        assert_eq!(result[1], batch2);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
#[serial]
fn test_roundtrip_arrow_array_stream_reader() -> anyhow::Result<()> {
    with_r(|| {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]))],
        )
        .map_err(|e| Error::Other(e.to_string()))?;

        let iter = RecordBatchIterator::new(vec![Ok(batch.clone())], schema);
        let stream = FFI_ArrowArrayStream::new(Box::new(iter) as Box<dyn RecordBatchReader + Send>);
        let reader =
            ArrowArrayStreamReader::try_new(stream).map_err(|e| Error::Other(e.to_string()))?;

        let robj = reader
            .into_arrow_robj()
            .map_err(|e| Error::Other(e.to_string()))?;

        let result = ArrowArrayStreamReader::from_arrow_robj(&robj)
            .map_err(|e| Error::Other(e.to_string()))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Error::Other(e.to_string()))?;

        assert_eq!(result.len(), 1);
        assert_eq!(result[0], batch);
        Ok::<(), extendr_api::Error>(())
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}

#[test]
fn test_ffi_field_direct() {
    use arrow::datatypes::{DataType, Field};
    use arrow::ffi::FFI_ArrowSchema;
    let field = Field::new("x", DataType::Int32, true);
    let ffi = FFI_ArrowSchema::try_from(&field).unwrap();
    let field2 = Field::try_from(&ffi).unwrap();
    assert_eq!(field, field2);
}

#[test]
fn test_ffi_field_boxed() {
    use arrow::datatypes::{DataType, Field};
    use arrow::ffi::FFI_ArrowSchema;
    let field = Field::new("x", DataType::Int32, true);
    let ffi = FFI_ArrowSchema::try_from(&field).unwrap();
    let ptr = Box::into_raw(Box::new(ffi));
    let schema = unsafe { &*ptr };
    let field2 = Field::try_from(schema).unwrap();
    unsafe {
        drop(Box::from_raw(ptr));
    }
    assert_eq!(field, field2);
}
