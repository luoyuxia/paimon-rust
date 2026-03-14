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

// RecordBatchReader iterates over Arrow record batches one at a time via
// the Arrow C Data Interface (zero-copy). Call Next to advance and Close
// when done.
//
//	reader, _ := read.ToArrow(plan)
//	defer reader.Close()
//	for {
//	    batch, err := reader.Next()
//	    if batch == nil { break }
//	    record, _ := cdata.ImportCRecordBatch(
//	        (*cdata.CArrowArray)(batch.Array),
//	        (*cdata.CArrowSchema)(batch.Schema),
//	    )
//	    // use record ...
//	    record.Release()
//	}
type RecordBatchReader struct {
	ctx   context.Context
	inner *paimonRecordBatchReader
}

// Next returns the next ArrowBatch, or (nil, nil) when iteration is complete.
func (r *RecordBatchReader) Next() (*ArrowBatch, error) {
	array, schema, err := ffiRecordBatchReaderNext.symbol(r.ctx)(r.inner)
	if err != nil {
		return nil, err
	}
	if array == nil && schema == nil {
		return nil, nil
	}
	ab := &ArrowBatch{ctx: r.ctx, Array: array, Schema: schema}
	runtime.SetFinalizer(ab, (*ArrowBatch).free)
	return ab, nil
}

// Close releases the underlying C record batch reader.
func (r *RecordBatchReader) Close() {
	ffiRecordBatchReaderFree.symbol(r.ctx)(r.inner)
}

// ToArrow creates a RecordBatchReader that lazily reads Arrow record batches
// from the given plan via the Arrow C Data Interface (zero-copy).
//
// The caller must call Close on the returned reader when done.
func (tr *TableRead) ToArrow(plan *Plan) (*RecordBatchReader, error) {
	reader, err := ffiTableReadToArrow.symbol(tr.ctx)(tr.inner, plan.inner)
	if err != nil {
		return nil, err
	}
	return &RecordBatchReader{ctx: tr.ctx, inner: reader}, nil
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
