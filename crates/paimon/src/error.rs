// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use snafu::prelude::*;

/// Result type used in paimon.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Error type for paimon.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(whatever, display("Paimon data invalid for {}: {:?}", message, source))]
    DataInvalid {
        message: String,
        /// see https://github.com/shepmaster/snafu/issues/446
        #[snafu(source(from(Box<dyn std::error::Error + Send + Sync + 'static>, Some)))]
        source: Option<Box<dyn std::error::Error + Send + 'static>>,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon data type invalid for {}", message)
    )]
    DataTypeInvalid { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected error {}: {:?}", message, source)
    )]
    IoUnexpected {
        message: String,
        source: opendal::Error,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unsupported io error {}", message)
    )]
    IoUnsupported { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting invalid config: {}", message)
    )]
    ConfigInvalid { message: String },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected avro error {}: {:?}", message, source)
    )]
    DataUnexpected {
        message: String,
        source: apache_avro::Error,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting unexpected parquet error: {}", message)
    )]
    ParquetDataUnexpected {
        message: String,
        source: parquet::errors::ParquetError,
    },
    #[snafu(
        visibility(pub(crate)),
        display("Paimon hitting invalid file index format: {}", message)
    )]
    FileIndexFormatInvalid { message: String },
}

impl From<opendal::Error> for Error {
    fn from(source: opendal::Error) -> Self {
        // TODO: Simple use IoUnexpected for now
        Error::IoUnexpected {
            message: "IO operation failed on underlying storage".to_string(),
            source,
        }
    }
}

impl From<apache_avro::Error> for Error {
    fn from(source: apache_avro::Error) -> Self {
        Error::DataUnexpected {
            message: "".to_string(),
            source,
        }
    }
}

impl From<parquet::errors::ParquetError> for Error {
    fn from(source: parquet::errors::ParquetError) -> Self {
        Error::ParquetDataUnexpected {
            message: format!("Failed to read a Parquet file: {}", source),
            source,
        }
    }
}
