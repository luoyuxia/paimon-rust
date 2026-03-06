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

# Spark with Paimon connector for integration tests.
# Writes Paimon tables to /tmp/paimon-warehouse (mount this as a volume
# so paimon-rust can read the data).
#
# Build args can be overridden via Makefile or docker build:
#   --build-arg PAIMON_VERSION=1.3.1
FROM apache/spark:3.5.3

# Re-declare ARGs so they are available in this stage (ARGs before FROM are not).
ARG PAIMON_VERSION

USER root
WORKDIR ${SPARK_HOME}

RUN apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Paimon Spark runtime JAR (Spark 3.5)
# https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.5/1.3.1/paimon-spark-3.5-1.3.1.jar
ENV PAIMON_JAR="paimon-spark-3.5-${PAIMON_VERSION}.jar"
ENV PAIMON_JAR_URL="https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-spark-3.5/${PAIMON_VERSION}/${PAIMON_JAR}"

RUN curl -fsSL --retry 3 -o "${SPARK_HOME}/jars/${PAIMON_JAR}" \
    "${PAIMON_JAR_URL}" && \
    chown -R spark:spark "${SPARK_HOME}/jars"

COPY spark-defaults.conf ${SPARK_HOME}/conf/
COPY provision.py /opt/provision.py
RUN chown spark:spark ${SPARK_HOME}/conf/spark-defaults.conf /opt/provision.py

USER spark

# Default: run provision script to create tables and write data to /tmp/paimon-warehouse.
# Override in docker-compose or when running manually.
# Use full path: entrypoint exec does not have SPARK_HOME/bin in PATH.
CMD ["/opt/spark/bin/spark-submit", "--master", "local[*]", "/opt/provision.py"]
