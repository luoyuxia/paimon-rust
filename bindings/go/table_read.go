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
	"errors"
	"io"
	"runtime"
	"sync"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/cdata"
	"github.com/jupiterrider/ffi"
)

// TableRead reads data from a table given a plan of splits.
type TableRead struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableRead
	closeOnce sync.Once
}

// Close releases the table read resources. Safe to call multiple times.
func (tr *TableRead) Close() {
	tr.closeOnce.Do(func() {
		ffiTableReadFree.symbol(tr.ctx)(tr.inner)
		tr.lib.release()
	})
}

// RecordBatchReader iterates over Arrow record batches one at a time via
// the Arrow C Data Interface (zero-copy). Call NextRecord to advance and
// Close when done.
//
//	reader, _ := read.ToRecordBatchReader(plan.Splits())
//	defer reader.Close()
//	for {
//	    record, err := reader.NextRecord()
//	    if err != nil { break } // io.EOF at end
//	    // use record ...
//	    record.Release()
//	}
type RecordBatchReader struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonRecordBatchReader
	closeOnce sync.Once
}

// NextRecord returns the next Arrow record, or io.EOF when iteration is
// complete. The underlying C batch is imported via the Arrow C Data Interface
// and released automatically — the caller only needs to call Release on the
// returned arrow.Record when done.
func (r *RecordBatchReader) NextRecord() (arrow.Record, error) {
	batch, err := r.next()
	if err != nil {
		return nil, err
	}
	record, err := cdata.ImportCRecordBatch(
		(*cdata.CArrowArray)(batch.array),
		(*cdata.CArrowSchema)(batch.schema),
	)
	batch.release()
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (r *RecordBatchReader) next() (*arrowBatch, error) {
	array, schema, err := ffiRecordBatchReaderNext.symbol(r.ctx)(r.inner)
	if err != nil {
		return nil, err
	}
	if array == nil && schema == nil {
		return nil, io.EOF
	}
	r.lib.acquire()
	ab := &arrowBatch{ctx: r.ctx, lib: r.lib, array: array, schema: schema}
	runtime.SetFinalizer(ab, (*arrowBatch).release)
	return ab, nil
}

// Close releases the underlying C record batch reader. Safe to call multiple times.
func (r *RecordBatchReader) Close() {
	r.closeOnce.Do(func() {
		ffiRecordBatchReaderFree.symbol(r.ctx)(r.inner)
		r.lib.release()
	})
}

// ToRecordBatchReader creates a RecordBatchReader that lazily reads Arrow
// record batches from the given splits via the Arrow C Data Interface
// (zero-copy).
//
// The splits select a contiguous index range within a plan. Order of
// elements in the slice does not matter; the range is determined by the
// minimum and maximum indices. Duplicate or non-contiguous indices return
// an error. All splits must originate from the same Plan.
//
// The caller must call Close on the returned reader when done.
func (tr *TableRead) ToRecordBatchReader(splits []DataSplit) (*RecordBatchReader, error) {
	if len(splits) == 0 {
		return nil, errors.New("paimon: no splits provided")
	}
	handle := splits[0].set.handle
	minIdx := splits[0].index
	maxIdx := splits[0].index
	for _, s := range splits[1:] {
		if s.set.handle != handle {
			return nil, errors.New("paimon: all splits must be from the same plan")
		}
		if s.index < minIdx {
			minIdx = s.index
		}
		if s.index > maxIdx {
			maxIdx = s.index
		}
	}
	if maxIdx-minIdx+1 != len(splits) {
		return nil, errors.New("paimon: splits must be contiguous (no gaps or duplicates)")
	}
	offset := uintptr(minIdx)
	length := uintptr(len(splits))
	reader, err := ffiTableReadToArrow.symbol(tr.ctx)(tr.inner, handle.inner, offset, length)
	if err != nil {
		return nil, err
	}
	tr.lib.acquire()
	return &RecordBatchReader{ctx: tr.ctx, lib: tr.lib, inner: reader}, nil
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
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(read *paimonTableRead, plan *paimonPlan, offset uintptr, length uintptr) (*paimonRecordBatchReader, error) {
	return func(read *paimonTableRead, plan *paimonPlan, offset uintptr, length uintptr) (*paimonRecordBatchReader, error) {
		var result resultRecordBatchReader
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&read),
			unsafe.Pointer(&plan),
			unsafe.Pointer(&offset),
			unsafe.Pointer(&length),
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
