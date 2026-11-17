//! POC: opening a Neo4j GDS graph from in-memory Arrow IPC bytes.

use std::ffi::CStr;
use std::os::raw::c_char;
use std::ptr;

use crate::abi::relate_status;
use crate::relate_free_string;
use crate::tabular::*;

/// Open `bytes`, returning the handle or the error message.
unsafe fn open(bytes: *const u8, len: usize) -> Result<*mut relate_database, String> {
    let mut db: *mut relate_database = ptr::null_mut();
    let mut error_json: *mut c_char = ptr::null_mut();
    // SAFETY: the caller passes a pointer readable for `len` bytes, or
    // null; the out-params are owned here.
    let status = unsafe { relate_database_open_gds_arrow(bytes, len, &mut db, &mut error_json) };
    if matches!(status, relate_status::RELATE_OK) {
        assert!(error_json.is_null());
        return Ok(db);
    }
    assert!(db.is_null());
    // SAFETY: a failed open fills `error_json` with a C string this side frees.
    let message = unsafe { CStr::from_ptr(error_json) }
        .to_str()
        .unwrap()
        .to_owned();
    // SAFETY: `error_json` came from the library.
    unsafe { relate_free_string(error_json) };
    Err(message)
}

#[test]
fn null_and_malformed_bytes_are_errors() {
    // SAFETY: a null pointer and an owned slice.
    unsafe {
        assert!(open(ptr::null(), 0).unwrap_err().contains("null bytes"));
        let junk = b"not arrow";
        assert!(open(junk.as_ptr(), junk.len()).is_err());
        assert!(matches!(
            relate_database_open_gds_arrow(
                junk.as_ptr(),
                junk.len(),
                ptr::null_mut(),
                ptr::null_mut()
            ),
            relate_status::RELATE_ERROR
        ));
    }
}

#[cfg(feature = "gds-codec")]
#[test]
fn a_gds_graph_opens_from_bytes_read_only() {
    use crate::result::*;
    use crate::value::*;
    use relate_storage::gds_fixture::{GdsExport, PropertyColumn, RelType};
    use std::ffi::CString;

    let dir = tempfile::tempdir().unwrap();
    GdsExport {
        labels: vec!["Person".into()],
        nodes: vec![(10, vec!["Person".into()]), (11, vec!["Person".into()])],
        node_properties: vec![("age".into(), PropertyColumn::Long(vec![Some(30), Some(40)]))],
        rel_types: vec![RelType {
            name: "KNOWS".into(),
            undirected: false,
            property_keys: vec!["since".into()],
            edges: vec![(0, 1, vec![2001.0])],
        }],
    }
    .write(dir.path(), 1);
    let mut bytes = std::fs::read(dir.path().join("graph.arrow")).unwrap();
    let read = CString::new(
        "MATCH (a:Person)-[k:KNOWS]->(b) RETURN a.originalId + b.originalId + toInteger(k.since)",
    )
    .unwrap();
    let write = CString::new("CREATE ()").unwrap();
    // SAFETY: test drives the FFI with handles, strings and out-params it
    // owns.
    unsafe {
        let db = open(bytes.as_ptr(), bytes.len()).unwrap();
        // The caller's buffer is free to change once the open returns.
        bytes.fill(0);
        drop(bytes);
        let mut conn: *mut relate_connection = ptr::null_mut();
        assert!(matches!(
            relate_connect_namespace(db, ptr::null(), &mut conn, ptr::null_mut()),
            relate_status::RELATE_OK
        ));
        let mut result: *mut relate_query_result = ptr::null_mut();
        assert!(matches!(
            relate_query_params(
                conn,
                read.as_ptr(),
                ptr::null(),
                &mut result,
                ptr::null_mut()
            ),
            relate_status::RELATE_OK
        ));
        assert_eq!(relate_result_is_error(result), 0);
        assert_eq!(relate_row_count(result), 1);
        let value = relate_value_at(result, 0, 0);
        let mut total = 0i64;
        assert!(matches!(
            relate_value_int(value, &mut total),
            relate_status::RELATE_OK
        ));
        assert_eq!(total, 10 + 11 + 2001);
        relate_destroy_result(result);

        let mut failed: *mut relate_query_result = ptr::null_mut();
        assert!(matches!(
            relate_query_params(
                conn,
                write.as_ptr(),
                ptr::null(),
                &mut failed,
                ptr::null_mut()
            ),
            relate_status::RELATE_OK
        ));
        assert_eq!(relate_result_is_error(failed), 1);
        relate_destroy_result(failed);
        relate_disconnect(conn);
        relate_database_close(db);
    }
}

/// The storage error is masked by the boundary like any other, so the
/// refusal carries no codec name.
#[cfg(not(feature = "gds-codec"))]
#[test]
fn a_build_without_the_codec_refuses_the_open() {
    let bytes = b"ARROW1";
    // SAFETY: an owned slice.
    assert!(unsafe { open(bytes.as_ptr(), bytes.len()) }.is_err());
}
