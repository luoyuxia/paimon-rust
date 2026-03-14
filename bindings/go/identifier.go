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
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Identifier identifies a table by database and object name.
type Identifier struct {
	ctx   context.Context
	inner *paimonIdentifier
}

// NewIdentifier creates a new Identifier with the given database and object name.
func (p *Paimon) NewIdentifier(database, object string) (*Identifier, error) {
	createFn := ffiIdentifierNew.symbol(p.ctx)
	inner, err := createFn(database, object)
	if err != nil {
		return nil, err
	}
	return &Identifier{ctx: p.ctx, inner: inner}, nil
}

// Close releases the identifier resources.
func (id *Identifier) Close() {
	ffiIdentifierFree.symbol(id.ctx)(id.inner)
}

var ffiIdentifierNew = newFFI(ffiOpts{
	sym:    "paimon_identifier_new",
	rType:  &typeResultIdentifierNew,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(database, object string) (*paimonIdentifier, error) {
	return func(database, object string) (*paimonIdentifier, error) {
		byteDB, err := BytePtrFromString(database)
		if err != nil {
			return nil, err
		}
		byteObj, err := BytePtrFromString(object)
		if err != nil {
			return nil, err
		}
		var result resultIdentifierNew
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&byteDB),
			unsafe.Pointer(&byteObj),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.identifier, nil
	}
})

var ffiIdentifierFree = newFFI(ffiOpts{
	sym:    "paimon_identifier_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(id *paimonIdentifier) {
	return func(id *paimonIdentifier) {
		ffiCall(
			nil,
			unsafe.Pointer(&id),
		)
	}
})
