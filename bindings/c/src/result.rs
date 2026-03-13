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

use crate::error::paimon_error;
use crate::types::*;

#[repr(C)]
pub struct paimon_result_catalog_new {
    pub catalog: *mut paimon_catalog,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_get_table {
    pub table: *mut paimon_table,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_new_read {
    pub read: *mut paimon_table_read,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_plan {
    pub plan: *mut paimon_plan,
    pub error: *mut paimon_error,
}

#[repr(C)]
pub struct paimon_result_to_arrow {
    /// Pointer to an array of paimon_arrow_batch.
    pub batches: *mut paimon_arrow_batch,
    /// Number of batches.
    pub num_batches: usize,
    pub error: *mut paimon_error,
}
