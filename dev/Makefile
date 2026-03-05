# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Alternative to docker-compose when buildx is missing: build and run with
# plain docker build + docker run.

IMAGE_NAME ?= spark-paimon
WAREHOUSE  ?= $(CURDIR)/warehouse

.PHONY: build run provision

# Use legacy builder to avoid requiring docker-buildx plugin
build:
	DOCKER_BUILDKIT=0 docker build -t $(IMAGE_NAME) ./spark

# Run provision (writes into ./warehouse). Requires: make build
run: build
	mkdir -p $(WAREHOUSE)
	docker run --rm -v $(WAREHOUSE):/tmp/paimon-warehouse $(IMAGE_NAME)

# Alias for run
provision: run
