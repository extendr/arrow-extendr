use anyhow::anyhow;
use arrow::{
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::FFI_ArrowArrayStream,
};
use extendr_api::prelude::*;
use extendr_ffi::{
    R_ExternalPtrAddr, R_ExternalPtrTag, R_MakeExternalPtr, R_NilValue, R_RegisterCFinalizerEx,
    Rf_protect, Rf_unprotect,
};

pub(crate) fn infer_schema_array(x: Robj) -> anyhow::Result<Robj> {
    let v = unsafe { Robj::from_sexp(R_ExternalPtrTag(x.get())) };
    if v.is_null() | !v.inherits("nanoarrow_schema") {
        Err(anyhow!("expected `nanoarrow_schema`"))
    } else {
        Ok(v)
    }
}

/// Returns a pointer to the `ArrowSchema` stored inside a `nanoarrow_schema` R external pointer.
/// The pointer is valid for as long as `schema_xptr` is alive.
pub(crate) fn c_export_schema(schema_xptr: &Robj) -> anyhow::Result<&FFI_ArrowSchema> {
    let ptr = unsafe { R_ExternalPtrAddr(schema_xptr.get()) as *const FFI_ArrowSchema };
    if ptr.is_null() {
        return Err(anyhow!("nanoarrow_schema pointer is NULL"));
    }
    Ok(unsafe { &*ptr })
}

unsafe extern "C" fn finalizer_schema(sexp: extendr_ffi::SEXP) {
    let ptr = unsafe { R_ExternalPtrAddr(sexp) } as *mut FFI_ArrowSchema;
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

pub(crate) fn schema_to_robj(schema: FFI_ArrowSchema) -> Robj {
    let ptr = Box::into_raw(Box::new(schema));
    let sexp = unsafe {
        let xptr = Rf_protect(R_MakeExternalPtr(ptr as *mut _, R_NilValue, R_NilValue));
        R_RegisterCFinalizerEx(xptr, Some(finalizer_schema), extendr_ffi::Rboolean::FALSE);
        xptr
    };
    let mut robj = unsafe { Robj::from_sexp(sexp) };
    robj.set_class(["nanoarrow_schema"])
        .expect("set nanoarrow_schema class");
    unsafe { Rf_unprotect(1) };
    robj
}

unsafe extern "C" fn finalizer_array(sexp: extendr_ffi::SEXP) {
    let ptr = unsafe { R_ExternalPtrAddr(sexp) } as *mut FFI_ArrowArray;
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

pub(crate) fn array_to_robj(array: FFI_ArrowArray, schema: Robj) -> Robj {
    let ptr = Box::into_raw(Box::new(array));
    let sexp = unsafe {
        // Protect schema before R_MakeExternalPtr, which allocates and can trigger GC.
        // Without this, the schema SEXP can be collected before it becomes the TAG.
        Rf_protect(schema.get());
        let xptr = Rf_protect(R_MakeExternalPtr(ptr as *mut _, schema.get(), R_NilValue));
        R_RegisterCFinalizerEx(xptr, Some(finalizer_array), extendr_ffi::Rboolean::FALSE);
        xptr
    };
    let mut robj = unsafe { Robj::from_sexp(sexp) };
    robj.set_class(["nanoarrow_array"])
        .expect("set nanoarrow_array class");
    unsafe { Rf_unprotect(2) };
    robj
}

unsafe extern "C" fn finalizer_array_stream(sexp: extendr_ffi::SEXP) {
    let ptr = unsafe { R_ExternalPtrAddr(sexp) } as *mut FFI_ArrowArrayStream;
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr)) };
    }
}

pub(crate) fn array_stream_to_robj(stream: FFI_ArrowArrayStream) -> Robj {
    let ptr = Box::into_raw(Box::new(stream));
    let sexp = unsafe {
        let xptr = Rf_protect(R_MakeExternalPtr(ptr as *mut _, R_NilValue, R_NilValue));
        R_RegisterCFinalizerEx(
            xptr,
            Some(finalizer_array_stream),
            extendr_ffi::Rboolean::FALSE,
        );
        xptr
    };
    let mut robj = unsafe { Robj::from_sexp(sexp) };
    robj.set_class(["nanoarrow_array_stream"])
        .expect("set nanoarrow_array_stream class");
    unsafe { Rf_unprotect(1) };
    robj
}

pub(crate) fn c_export_array(array_xptr: &Robj) -> anyhow::Result<FFI_ArrowArray> {
    let ptr = unsafe { R_ExternalPtrAddr(array_xptr.get()) as *mut FFI_ArrowArray };
    if ptr.is_null() {
        return Err(anyhow!("nanoarrow_array pointer is NULL"));
    }
    Ok(unsafe { FFI_ArrowArray::from_raw(ptr) })
}

pub(crate) fn c_export_array_stream(stream_xptr: &Robj) -> anyhow::Result<FFI_ArrowArrayStream> {
    let ptr = unsafe { R_ExternalPtrAddr(stream_xptr.get()) as *mut FFI_ArrowArrayStream };
    if ptr.is_null() {
        return Err(anyhow!("nanoarrow_array_stream pointer is NULL"));
    }
    Ok(unsafe { FFI_ArrowArrayStream::from_raw(ptr) })
}
