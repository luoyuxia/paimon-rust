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

// Package paimon provides a Go binding for Apache Paimon Rust.
//
// This binding uses purego and libffi to call into the paimon-c shared library.
// The pre-built shared library is embedded in the package and automatically
// loaded at runtime — no manual build step needed.
//
// This package requires CGO because it imports the arrow-go cdata package
// for Arrow C Data Interface support.
//
// Basic usage:
//
//	p, err := paimon.Open()
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer p.Close()
//
//	catalog, err := p.NewFileSystemCatalog("/path/to/warehouse")
//	...
package paimon

import (
	"context"
	"sync"
)

// Paimon is the entry point for all paimon operations.
// Create one with Open() or OpenLibrary().
//
// Paimon must outlive all objects derived from it (Catalog, Table, etc.),
// or those objects must be closed first. The underlying shared library is
// reference-counted and will not be unloaded until all derived objects
// are closed.
type Paimon struct {
	ctx       context.Context
	lib       *libRef
	closeOnce sync.Once
}

// Open loads the embedded paimon-c shared library and returns a Paimon instance.
// The library is decompressed from the embedded binary on first call and
// cached for subsequent calls.
func Open() (*Paimon, error) {
	if err := loadEmbeddedLib(); err != nil {
		return nil, err
	}
	return OpenLibrary(libPath)
}

// OpenLibrary loads a paimon-c shared library from an explicit filesystem path.
// Use this for development when working with a locally built library.
func OpenLibrary(path string) (*Paimon, error) {
	ctx, lib, err := newContext(path)
	if err != nil {
		return nil, err
	}
	return &Paimon{ctx: ctx, lib: lib}, nil
}

// Close releases this Paimon instance's reference to the shared library.
// The library is unloaded once all derived objects are also closed.
// Close is safe to call multiple times.
func (p *Paimon) Close() {
	p.closeOnce.Do(func() {
		p.lib.release()
	})
}
