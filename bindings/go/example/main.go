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

package main

import (
	"fmt"
	"log"
	"os"

	paimon "github.com/apache/paimon-rust/bindings/go"
)

func main() {
	warehouse := os.Getenv("PAIMON_WAREHOUSE")
	if warehouse == "" {
		warehouse = "/tmp/paimon-warehouse"
	}

	// 1. Open paimon — the embedded shared library is loaded automatically.
	p, err := paimon.Open()
	if err != nil {
		log.Fatalf("Failed to open paimon: %v", err)
	}
	defer p.Close()
	fmt.Println("Paimon library loaded.")

	// 2. Create a FileSystemCatalog.
	catalog, err := p.NewFileSystemCatalog(warehouse)
	if err != nil {
		log.Fatalf("Failed to create catalog: %v", err)
	}
	defer catalog.Close()
	fmt.Printf("Catalog created for warehouse: %s\n", warehouse)

	// 3. Create an Identifier for the table.
	identifier, err := p.NewIdentifier("default", "simple_log_table")
	if err != nil {
		log.Fatalf("Failed to create identifier: %v", err)
	}
	defer identifier.Close()

	// 4. Get the table from the catalog.
	table, err := catalog.GetTable(identifier)
	if err != nil {
		log.Fatalf("Failed to get table: %v", err)
	}
	defer table.Close()
	fmt.Println("Table retrieved: default.simple_log_table")

	// 5. Create a ReadBuilder.
	readBuilder := table.NewReadBuilder()
	defer readBuilder.Close()

	// 6. Create a TableScan and execute the scan plan.
	scan, err := readBuilder.NewScan()
	if err != nil {
		log.Fatalf("Failed to create table scan: %v", err)
	}
	defer scan.Close()

	plan, err := scan.Plan()
	if err != nil {
		log.Fatalf("Failed to plan scan: %v", err)
	}
	defer plan.Close()
	fmt.Println("Scan plan created.")

	// 7. Create a TableRead.
	read, err := readBuilder.NewRead()
	if err != nil {
		log.Fatalf("Failed to create table read: %v", err)
	}
	defer read.Close()

	// 8. Read data as Arrow record batches (zero-copy via C Data Interface).
	data, err := read.ToArrow(plan)
	if err != nil {
		log.Fatalf("Failed to read arrow data: %v", err)
	}

	fmt.Printf("Read %d Arrow record batches.\n", len(data))

	// Each ArrowBatch contains Array and Schema pointers that can be
	// imported using arrow-go's cdata package:
	//
	//   import "github.com/apache/arrow-go/v18/arrow/cdata"
	//
	//   for _, batch := range data {
	//       record, _ := cdata.ImportCRecordBatch(
	//           (*cdata.CArrowArray)(batch.Array),
	//           (*cdata.CArrowSchema)(batch.Schema),
	//       )
	//       defer record.Release()
	//       fmt.Println(record)
	//   }
}
