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

// nanoarrow::infer_nanoarrow_schema()
// SEXP nanoarrow_c_infer_schema_array(SEXP array_xptr) {
//   SEXP maybe_schema_xptr = R_ExternalPtrTag(array_xptr);
//   if (Rf_inherits(maybe_schema_xptr, "nanoarrow_schema")) {
//     return maybe_schema_xptr;
//   } else {
//     return R_NilValue;
//   }
// }
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

// nanoarrow::nanoarrow_pointer_export()`
// function (ptr_src, ptr_dst)
// {
//     if (inherits(ptr_src, "nanoarrow_schema")) {
//         .Call(nanoarrow_c_export_schema, ptr_src, ptr_dst)
//     }
//     else if (inherits(ptr_src, "nanoarrow_array")) {
//         .Call(nanoarrow_c_export_array, ptr_src, ptr_dst)
//     }
//     else if (inherits(ptr_src, "nanoarrow_array_stream")) {
//         .Call(nanoarrow_c_export_array_stream, ptr_src, ptr_dst)
//     }
//     else {
//         stop("`ptr_src` must inherit from 'nanoarrow_schema', 'nanoarrow_array', or 'nanoarrow_array_stream'")
//     }
//     invisible(ptr_dst)
// }
// SEXP nanoarrow_c_export_schema(SEXP schema_xptr, SEXP ptr_dst) {
//   struct ArrowSchema* obj_src = nanoarrow_schema_from_xptr(schema_xptr);
//   SEXP xptr_dst = PROTECT(nanoarrow_c_pointer(ptr_dst));

//   struct ArrowSchema* obj_dst = (struct ArrowSchema*)R_ExternalPtrAddr(xptr_dst);
//   if (obj_dst == NULL) {
//     Rf_error("`ptr_dst` is a pointer to NULL");
//   }

//   if (obj_dst->release != NULL) {
//     Rf_error("`ptr_dst` is a valid struct ArrowSchema");
//   }

//   int result = ArrowSchemaDeepCopy(obj_src, obj_dst);
//   if (result != NANOARROW_OK) {
//     Rf_error("Failed to deep copy struct ArrowSchema");
//   }

//   UNPROTECT(1);
//   return R_NilValue;
// }

// SEXP nanoarrow_c_export_array(SEXP array_xptr, SEXP ptr_dst) {
//   SEXP xptr_dst = PROTECT(nanoarrow_c_pointer(ptr_dst));

//   struct ArrowArray* obj_dst = (struct ArrowArray*)R_ExternalPtrAddr(xptr_dst);
//   if (obj_dst == NULL) {
//     Rf_error("`ptr_dst` is a pointer to NULL");
//   }

//   if (obj_dst->release != NULL) {
//     Rf_error("`ptr_dst` is a valid struct ArrowArray");
//   }

//   array_export(array_xptr, obj_dst);
//   UNPROTECT(1);
//   return R_NilValue;
// }

// SEXP nanoarrow_c_export_array_stream(SEXP array_stream_xptr, SEXP ptr_dst) {
//   SEXP xptr_dst = PROTECT(nanoarrow_c_pointer(ptr_dst));

//   struct ArrowArrayStream* obj_dst =
//       (struct ArrowArrayStream*)R_ExternalPtrAddr(xptr_dst);
//   if (obj_dst == NULL) {
//     Rf_error("`ptr_dst` is a pointer to NULL");
//   }

//   if (obj_dst->release != NULL) {
//     Rf_error("`ptr_dst` is a valid struct ArrowArrayStream");
//   }

//   array_stream_export(array_stream_xptr, obj_dst);

//   // Remove SEXP dependencies (if important they are kept alive by array_stream_export)
//   R_SetExternalPtrProtected(array_stream_xptr, R_NilValue);
//   R_SetExternalPtrTag(array_stream_xptr, R_NilValue);

//   UNPROTECT(1);
//   return R_NilValue;
// }
