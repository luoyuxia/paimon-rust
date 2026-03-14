/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package paimon

import (
	"context"
	"runtime"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// TableRead reads data from a table given a plan of splits.
type TableRead struct {
	ctx   context.Context
	inner *paimonTableRead
}

// Close releases the table read resources.
func (tr *TableRead) Close() {
	ffiTableReadFree.symbol(tr.ctx)(tr.inner)
}

// ToArrow reads data from the given plan and returns Arrow record batches
// via the Arrow C Data Interface (zero-copy).
//
// Each ArrowBatch contains Array and Schema pointers that can be imported
// using arrow-go's cdata package. The ArrowBatch container structs are
// freed automatically by the GC — no manual Close is needed.
//
//	batches, _ := read.ToArrow(plan)
//	for _, batch := range batches {
//	    record, _ := cdata.ImportCRecordBatch(
//	        (*cdata.CArrowArray)(batch.Array),
//	        (*cdata.CArrowSchema)(batch.Schema),
//	    )
//	    defer record.Release()
//	}
func (tr *TableRead) ToArrow(plan *Plan) ([]*ArrowBatch, error) {
	createFn := ffiTableReadToArrow.symbol(tr.ctx)
	reader, err := createFn(tr.inner, plan.inner)
	if err != nil {
		return nil, err
	}
	defer ffiRecordBatchReaderFree.symbol(tr.ctx)(reader)

	nextFn := ffiRecordBatchReaderNext.symbol(tr.ctx)
	var result []*ArrowBatch
	for {
		array, schema, err := nextFn(reader)
		if err != nil {
			return nil, err
		}
		if array == nil && schema == nil {
			break
		}
		ab := &ArrowBatch{ctx: tr.ctx, Array: array, Schema: schema}
		runtime.SetFinalizer(ab, (*ArrowBatch).free)
		result = append(result, ab)
	}
	return result, nil
}

var ffiTableReadFree = newFFI(ffiOpts{
	sym:    "paimon_table_read_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(read *paimonTableRead) {
	return func(read *paimonTableRead) {
		ffiCall(
			nil,
			unsafe.Pointer(&read),
		)
	}
})

var ffiTableReadToArrow = newFFI(ffiOpts{
	sym:    "paimon_table_read_to_arrow",
	rType:  &typeResultRecordBatchReader,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(read *paimonTableRead, plan *paimonPlan) (*paimonRecordBatchReader, error) {
	return func(read *paimonTableRead, plan *paimonPlan) (*paimonRecordBatchReader, error) {
		var result resultRecordBatchReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&read),
			unsafe.Pointer(&plan),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.reader, nil
	}
})

var ffiRecordBatchReaderNext = newFFI(ffiOpts{
	sym:    "paimon_record_batch_reader_next",
	rType:  &typeResultNextBatch,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(reader *paimonRecordBatchReader) (unsafe.Pointer, unsafe.Pointer, error) {
	return func(reader *paimonRecordBatchReader) (unsafe.Pointer, unsafe.Pointer, error) {
		var result resultNextBatch
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&reader),
		)
		if result.error != nil {
			return nil, nil, parseError(ctx, result.error)
		}
		return result.array, result.schema, nil
	}
})

var ffiRecordBatchReaderFree = newFFI(ffiOpts{
	sym:    "paimon_record_batch_reader_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(reader *paimonRecordBatchReader) {
	return func(reader *paimonRecordBatchReader) {
		ffiCall(
			nil,
			unsafe.Pointer(&reader),
		)
	}
})

var ffiArrowBatchFree = newFFI(ffiOpts{
	sym:    "paimon_arrow_batch_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&typeArrowBatch},
}, func(_ context.Context, ffiCall ffiCall) func(array unsafe.Pointer, schema unsafe.Pointer) {
	return func(array unsafe.Pointer, schema unsafe.Pointer) {
		batch := struct {
			array  unsafe.Pointer
			schema unsafe.Pointer
		}{array: array, schema: schema}
		ffiCall(
			nil,
			unsafe.Pointer(&batch),
		)
	}
})
