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

package paimon_test

import (
	"errors"
	"io"
	"os"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	paimon "github.com/apache/paimon-rust/bindings/go"
)

// TestReadLogTable reads the test table and verifies the data matches expected values.
//
// The table was populated by Docker provisioning with:
//
//	(1, 'alice'), (2, 'bob'), (3, 'carol')
func TestReadLogTable(t *testing.T) {
	warehouse := os.Getenv("PAIMON_TEST_WAREHOUSE")
	if warehouse == "" {
		warehouse = "/Users/yuxia/Projects/rust-projects/paimon-rust/dev/paimon-warehouse"
	}

	if _, err := os.Stat(warehouse); os.IsNotExist(err) {
		t.Skipf("Skipping: warehouse %s does not exist (run 'make docker-up' first)", warehouse)
	}

	p, err := paimon.Open()
	if err != nil {
		t.Fatalf("Failed to open paimon: %v", err)
	}
	defer p.Close()

	catalog, err := p.NewFileSystemCatalog(warehouse)
	if err != nil {
		t.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()

	identifier, err := p.NewIdentifier("default", "simple_log_table")
	if err != nil {
		t.Fatalf("Failed to create identifier: %v", err)
	}
	defer identifier.Close()

	table, err := catalog.GetTable(identifier)
	if err != nil {
		t.Fatalf("Failed to get table: %v", err)
	}
	defer table.Close()

	readBuilder, err := table.NewReadBuilder()
	if err != nil {
		t.Fatalf("Failed to create read builder: %v", err)
	}
	defer readBuilder.Close()

	scan, err := readBuilder.NewScan()
	if err != nil {
		t.Fatalf("Failed to create scan: %v", err)
	}
	defer scan.Close()

	plan, err := scan.Plan()
	if err != nil {
		t.Fatalf("Failed to plan: %v", err)
	}
	defer plan.Close()

	read, err := readBuilder.NewRead()
	if err != nil {
		t.Fatalf("Failed to create read: %v", err)
	}
	defer read.Close()

	reader, err := read.ToRecordBatchReader(plan.Splits())
	if err != nil {
		t.Fatalf("Failed to read arrow: %v", err)
	}
	defer reader.Close()

	// Import Arrow batches via C Data Interface and collect rows.
	// Strings are copied before Release because arrow-go's String.Value()
	// returns zero-copy references into the Arrow buffer.
	type row struct {
		id   int32
		name string
	}
	var rows []row
	batchIdx := 0
	for {
		record, err := reader.NextRecord()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Batch %d: failed to read next record: %v", batchIdx, err)
		}

		idIdx := record.Schema().FieldIndices("id")
		nameIdx := record.Schema().FieldIndices("name")
		if len(idIdx) == 0 || len(nameIdx) == 0 {
			record.Release()
			t.Fatalf("Batch %d: missing expected columns (id, name) in schema: %s", batchIdx, record.Schema())
		}

		idCol := record.Column(idIdx[0]).(*array.Int32)
		nameCol := record.Column(nameIdx[0]).(*array.String)

		for j := 0; j < int(record.NumRows()); j++ {
			rows = append(rows, row{
				id:   idCol.Value(j),
				name: string([]byte(nameCol.Value(j))),
			})
		}
		record.Release()
		batchIdx++
	}

	if len(rows) == 0 {
		t.Fatal("Expected at least one row, got 0")
	}

	sort.Slice(rows, func(i, j int) bool {
		return rows[i].id < rows[j].id
	})

	expected := []row{
		{1, "alice"},
		{2, "bob"},
		{3, "carol"},
	}

	if len(rows) != len(expected) {
		t.Fatalf("Expected %d rows, got %d: %v", len(expected), len(rows), rows)
	}

	for i, exp := range expected {
		if rows[i] != exp {
			t.Errorf("Row %d: expected %v, got %v", i, exp, rows[i])
		}
	}
}
