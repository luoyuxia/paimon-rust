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

use std::future::Future;

fn block_on_new_runtime<F>(future: F, runtime_error: &'static str) -> F::Output
where
    F: Future,
{
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect(runtime_error)
        .block_on(future)
}

// These helpers work around DataFusion FFI callbacks that may run without an
// entered Tokio runtime. See https://github.com/apache/datafusion/issues/16312.
// Creating a new Tokio runtime here is only a fallback for that gap; if
// DataFusion fixes runtime propagation end-to-end, we should be able to remove
// these manual fallback runtimes.
pub(crate) async fn await_with_runtime<F>(future: F, runtime_error: &'static str) -> F::Output
where
    F: Future,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        future.await
    } else {
        block_on_new_runtime(future, runtime_error)
    }
}

// The blocking variant is for synchronous DataFusion FFI callbacks such as
// CatalogProvider::schema(), where we cannot `.await` directly.
pub(crate) fn block_on_with_runtime<F>(
    future: F,
    runtime_error: &'static str,
    panic_error: &'static str,
) -> F::Output
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let run = move || block_on_new_runtime(future, runtime_error);

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::spawn(run).join().expect(panic_error)
    } else {
        run()
    }
}
