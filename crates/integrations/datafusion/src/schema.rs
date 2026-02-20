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

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::DataFusionError;
use datafusion::common::Result as DFResult;
use std::sync::Arc;

use paimon::spec::{DataField, DataType as PaimonDataType};

/// Converts Paimon table schema (logical row type fields) to DataFusion Arrow schema.
pub fn paimon_schema_to_arrow(fields: &[DataField]) -> DFResult<Arc<Schema>> {
    let arrow_fields: Vec<Field> = fields
        .iter()
        .map(|f| {
            let arrow_type = paimon_data_type_to_arrow(f.data_type())?;
            Ok(Field::new(
                f.name(),
                arrow_type,
                f.data_type().is_nullable(),
            ))
        })
        .collect::<DFResult<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(arrow_fields)))
}

fn paimon_data_type_to_arrow(dt: &PaimonDataType) -> DFResult<DataType> {
    use datafusion::arrow::datatypes::TimeUnit;

    Ok(match dt {
        PaimonDataType::Boolean(_) => DataType::Boolean,
        PaimonDataType::TinyInt(_) => DataType::Int8,
        PaimonDataType::SmallInt(_) => DataType::Int16,
        PaimonDataType::Int(_) => DataType::Int32,
        PaimonDataType::BigInt(_) => DataType::Int64,
        PaimonDataType::Float(_) => DataType::Float32,
        PaimonDataType::Double(_) => DataType::Float64,
        PaimonDataType::VarChar(_) | PaimonDataType::Char(_) => DataType::Utf8,
        PaimonDataType::Binary(_) | PaimonDataType::VarBinary(_) => DataType::Binary,
        PaimonDataType::Date(_) => DataType::Date32,
        PaimonDataType::Time(_) => DataType::Time64(TimeUnit::Microsecond),
        PaimonDataType::Timestamp(_) | PaimonDataType::LocalZonedTimestamp(_) => {
            DataType::Timestamp(TimeUnit::Microsecond, None)
        }
        PaimonDataType::Decimal(d) => {
            let p = u8::try_from(d.precision()).map_err(|_| {
                DataFusionError::Internal("Decimal precision exceeds u8".to_string())
            })?;
            let s = i8::try_from(d.scale() as i32).map_err(|_| {
                DataFusionError::Internal("Decimal scale out of i8 range".to_string())
            })?;
            DataType::Decimal128(p, s)
        }
        PaimonDataType::Array(_)
        | PaimonDataType::Map(_)
        | PaimonDataType::Multiset(_)
        | PaimonDataType::Row(_) => {
            return Err(DataFusionError::NotImplemented(
                "Paimon DataFusion integration does not yet support nested types (Array/Map/Row)"
                    .to_string(),
            ));
        }
    })
}
