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
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Plan holds the scan result containing data splits to read.
type Plan struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonPlan
	closeOnce sync.Once
}

// Close releases the plan resources. Safe to call multiple times.
func (p *Plan) Close() {
	p.closeOnce.Do(func() {
		ffiPlanFree.symbol(p.ctx)(p.inner)
		p.lib.release()
	})
}

// DataSplit references a single data split inside a Plan.
// The parent Plan must remain open while the DataSplit is in use.
type DataSplit struct {
	plan  *Plan
	index int
}

// NumSplits returns the number of data splits in the plan.
func (p *Plan) NumSplits() int {
	return ffiPlanNumSplits.symbol(p.ctx)(p.inner)
}

// Splits returns all data splits in the plan.
func (p *Plan) Splits() []DataSplit {
	n := p.NumSplits()
	splits := make([]DataSplit, n)
	for i := range splits {
		splits[i] = DataSplit{plan: p, index: i}
	}
	return splits
}

var ffiPlanFree = newFFI(ffiOpts{
	sym:    "paimon_plan_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(plan *paimonPlan) {
	return func(plan *paimonPlan) {
		ffiCall(
			nil,
			unsafe.Pointer(&plan),
		)
	}
})

var ffiPlanNumSplits = newFFI(ffiOpts{
	sym:    "paimon_plan_num_splits",
	rType:  &ffi.TypePointer, // usize == pointer-sized on 64-bit
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(plan *paimonPlan) int {
	return func(plan *paimonPlan) int {
		var count uintptr
		ffiCall(
			unsafe.Pointer(&count),
			unsafe.Pointer(&plan),
		)
		return int(count)
	}
})
