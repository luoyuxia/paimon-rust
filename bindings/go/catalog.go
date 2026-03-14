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

// Catalog wraps a paimon FileSystemCatalog.
type Catalog struct {
	ctx   context.Context
	lib   *libRef
	inner *paimonCatalog
}

// NewFileSystemCatalog creates a new FileSystemCatalog for the given warehouse path.
func (p *Paimon) NewFileSystemCatalog(warehouse string) (*Catalog, error) {
	createFn := ffiCatalogNew.symbol(p.ctx)
	inner, err := createFn(warehouse)
	if err != nil {
		return nil, err
	}
	p.lib.acquire()
	return &Catalog{ctx: p.ctx, lib: p.lib, inner: inner}, nil
}

// Close releases the catalog resources.
func (c *Catalog) Close() {
	ffiCatalogFree.symbol(c.ctx)(c.inner)
	c.lib.release()
}

// GetTable retrieves a table from the catalog using the given identifier.
func (c *Catalog) GetTable(id *Identifier) (*Table, error) {
	getFn := ffiCatalogGetTable.symbol(c.ctx)
	inner, err := getFn(c.inner, id.inner)
	if err != nil {
		return nil, err
	}
	c.lib.acquire()
	return &Table{ctx: c.ctx, lib: c.lib, inner: inner}, nil
}

var ffiCatalogNew = newFFI(ffiOpts{
	sym:    "paimon_catalog_new",
	rType:  &typeResultCatalogNew,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(warehouse string) (*paimonCatalog, error) {
	return func(warehouse string) (*paimonCatalog, error) {
		byteWarehouse, err := BytePtrFromString(warehouse)
		if err != nil {
			return nil, err
		}
		var result resultCatalogNew
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&byteWarehouse),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.catalog, nil
	}
})

var ffiCatalogFree = newFFI(ffiOpts{
	sym:    "paimon_catalog_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(catalog *paimonCatalog) {
	return func(catalog *paimonCatalog) {
		ffiCall(
			nil,
			unsafe.Pointer(&catalog),
		)
	}
})

var ffiCatalogGetTable = newFFI(ffiOpts{
	sym:    "paimon_catalog_get_table",
	rType:  &typeResultGetTable,
	aTypes: []*ffi.Type{&ffi.TypePointer, &ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(catalog *paimonCatalog, id *paimonIdentifier) (*paimonTable, error) {
	return func(catalog *paimonCatalog, id *paimonIdentifier) (*paimonTable, error) {
		var result resultGetTable
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&catalog),
			unsafe.Pointer(&id),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.table, nil
	}
})
