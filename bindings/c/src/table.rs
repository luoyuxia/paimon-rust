// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::ffi::c_void;
use std::mem::ManuallyDrop;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, RecordBatch, StructArray};
use paimon::table::Table;
use paimon::Plan;

use crate::error::paimon_error;
use crate::result::{paimon_result_new_read, paimon_result_plan, paimon_result_to_arrow};
use crate::runtime;
use crate::types::*;

// Helper to box a Table clone into a wrapper struct and return a raw pointer.
unsafe fn box_table_wrapper<T>(table: &Table, make: impl FnOnce(*mut c_void) -> T) -> *mut T {
    let inner = Box::into_raw(Box::new(table.clone())) as *mut c_void;
    Box::into_raw(Box::new(make(inner)))
}

// Helper to free a wrapper struct that contains a Table clone.
unsafe fn free_table_wrapper<T>(ptr: *mut T, get_inner: impl FnOnce(&T) -> *mut c_void) {
    if !ptr.is_null() {
        let wrapper = Box::from_raw(ptr);
        let inner = get_inner(&wrapper);
        if !inner.is_null() {
            drop(Box::from_raw(inner as *mut Table));
        }
    }
}

// ======================= Table ===============================

/// Free a paimon_table.
///
/// # Safety
/// Only call with a table returned from `paimon_catalog_get_table`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_free(table: *mut paimon_table) {
    free_table_wrapper(table, |t| t.inner);
}

/// Create a new ReadBuilder from a Table.
///
/// # Safety
/// `table` must be a valid pointer from `paimon_catalog_get_table`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_new_read_builder(
    table: *const paimon_table,
) -> *mut paimon_read_builder {
    let table_ref = &*((*table).inner as *const Table);
    box_table_wrapper(table_ref, |inner| paimon_read_builder { inner })
}

// ======================= ReadBuilder ===============================

/// Free a paimon_read_builder.
///
/// # Safety
/// Only call with a read_builder returned from `paimon_table_new_read_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_free(rb: *mut paimon_read_builder) {
    free_table_wrapper(rb, |r| r.inner);
}

/// Create a new TableScan from a ReadBuilder.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_scan(
    rb: *const paimon_read_builder,
) -> *mut paimon_table_scan {
    let table = &*((*rb).inner as *const Table);
    box_table_wrapper(table, |inner| paimon_table_scan { inner })
}

/// Create a new TableRead from a ReadBuilder.
///
/// # Safety
/// `rb` must be a valid pointer from `paimon_table_new_read_builder`.
#[no_mangle]
pub unsafe extern "C" fn paimon_read_builder_new_read(
    rb: *const paimon_read_builder,
) -> paimon_result_new_read {
    let table = &*((*rb).inner as *const Table);
    let rb_rust = table.new_read_builder();
    match rb_rust.new_read() {
        Ok(_) => {
            let wrapper = box_table_wrapper(table, |inner| paimon_table_read { inner });
            paimon_result_new_read {
                read: wrapper,
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_new_read {
            read: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= TableScan ===============================

/// Free a paimon_table_scan.
///
/// # Safety
/// Only call with a scan returned from `paimon_read_builder_new_scan`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_scan_free(scan: *mut paimon_table_scan) {
    free_table_wrapper(scan, |s| s.inner);
}

/// Execute a scan plan to get splits.
///
/// # Safety
/// `scan` must be a valid pointer from `paimon_read_builder_new_scan`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_scan_plan(
    scan: *const paimon_table_scan,
) -> paimon_result_plan {
    let table = &*((*scan).inner as *const Table);
    let rb = table.new_read_builder();
    let table_scan = rb.new_scan();

    match runtime().block_on(table_scan.plan()) {
        Ok(plan) => {
            let wrapper = Box::new(paimon_plan {
                inner: Box::into_raw(Box::new(plan)) as *mut c_void,
            });
            paimon_result_plan {
                plan: Box::into_raw(wrapper),
                error: std::ptr::null_mut(),
            }
        }
        Err(e) => paimon_result_plan {
            plan: std::ptr::null_mut(),
            error: paimon_error::from_paimon(e),
        },
    }
}

// ======================= Plan ===============================

/// Free a paimon_plan.
///
/// # Safety
/// Only call with a plan returned from `paimon_table_scan_plan`.
#[no_mangle]
pub unsafe extern "C" fn paimon_plan_free(plan: *mut paimon_plan) {
    if !plan.is_null() {
        let p = Box::from_raw(plan);
        if !p.inner.is_null() {
            drop(Box::from_raw(p.inner as *mut Plan));
        }
    }
}

// ======================= TableRead ===============================

/// Free a paimon_table_read.
///
/// # Safety
/// Only call with a read returned from `paimon_read_builder_new_read`.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_free(read: *mut paimon_table_read) {
    free_table_wrapper(read, |r| r.inner);
}

/// Read table data as Arrow record batches via the Arrow C Data Interface (zero-copy).
///
/// Returns an array of `paimon_arrow_batch`, each containing a heap-allocated
/// ArrowArray and ArrowSchema pointer pair. After importing each batch (e.g.
/// via `arrow::ImportRecordBatch`), call `paimon_arrow_batch_free` to free the
/// container structs, then call `paimon_arrow_result_free` to free the array.
///
/// # Safety
/// `read` and `plan` must be valid pointers from previous paimon C calls.
#[no_mangle]
pub unsafe extern "C" fn paimon_table_read_to_arrow(
    read: *const paimon_table_read,
    plan: *const paimon_plan,
) -> paimon_result_to_arrow {
    let table = &*((*read).inner as *const Table);
    let plan_ref = &*((*plan).inner as *const Plan);

    match export_batches(table, plan_ref) {
        Ok((ptr, len)) => paimon_result_to_arrow {
            batches: ptr,
            num_batches: len,
            error: std::ptr::null_mut(),
        },
        Err(e) => paimon_result_to_arrow {
            batches: std::ptr::null_mut(),
            num_batches: 0,
            error: paimon_error::from_paimon(e),
        },
    }
}

fn export_batches(table: &Table, plan: &Plan) -> paimon::Result<(*mut paimon_arrow_batch, usize)> {
    let rb = table.new_read_builder();
    let read = rb.new_read()?;
    let stream = read.to_arrow(plan.splits())?;

    let batches: Vec<RecordBatch> = runtime().block_on(async {
        use futures::TryStreamExt;
        stream.try_collect().await
    })?;

    let mut ffi_batches = Vec::with_capacity(batches.len());
    for batch in batches {
        let schema = batch.schema();
        let struct_array = StructArray::from(batch);
        let ffi_array = FFI_ArrowArray::new(&struct_array.to_data());
        let ffi_schema = FFI_ArrowSchema::try_from(schema.as_ref()).map_err(|e| {
            paimon::Error::UnexpectedError {
                message: format!("Failed to export Arrow schema: {e}"),
                source: Some(Box::new(e)),
            }
        })?;

        let array_ptr = Box::into_raw(Box::new(ffi_array)) as *mut c_void;
        let schema_ptr = Box::into_raw(Box::new(ffi_schema)) as *mut c_void;

        ffi_batches.push(paimon_arrow_batch {
            array: array_ptr,
            schema: schema_ptr,
        });
    }

    let len = ffi_batches.len();
    let ptr = ManuallyDrop::new(ffi_batches).as_mut_ptr();
    Ok((ptr, len))
}

/// Free the ArrowArray and ArrowSchema container structs for a single batch.
///
/// # Safety
/// `batch` must contain valid pointers returned by `paimon_table_read_to_arrow`.
#[no_mangle]
pub unsafe extern "C" fn paimon_arrow_batch_free(batch: paimon_arrow_batch) {
    if !batch.array.is_null() {
        drop(Box::from_raw(batch.array as *mut FFI_ArrowArray));
    }
    if !batch.schema.is_null() {
        drop(Box::from_raw(batch.schema as *mut FFI_ArrowSchema));
    }
}

/// Free the batches array returned by `paimon_table_read_to_arrow`.
///
/// # Safety
/// `batches` and `num_batches` must come from a `paimon_result_to_arrow`.
#[no_mangle]
pub unsafe extern "C" fn paimon_arrow_result_free(
    batches: *mut paimon_arrow_batch,
    num_batches: usize,
) {
    if !batches.is_null() {
        drop(Vec::from_raw_parts(batches, num_batches, num_batches));
    }
}
