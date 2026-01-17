use crate::spec::{DataField, DataType, TableSchema};
use crate::{Error, Result};
use arrow_array::types::{validate_decimal_precision_and_scale, Decimal128Type};
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit};
use std::sync::Arc;
pub const UTC_TIME_ZONE: &str = "+00:00";
pub const DEFAULT_MAP_FIELD_NAME: &str = "key_value";

pub fn schema_to_arrow_schema(schema: &TableSchema) -> Result<Schema> {
    let mut results = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        results.push(data_field_to_arrow_field(field)?)
    }
    Ok(Schema::new(results))
}

/// Convert Paimon timestamp/time precision to Arrow TimeUnit.
///
/// Precision mapping:
/// - 0 -> Second (no fractional seconds)
/// - 1-3 -> Millisecond (1-3 fractional digits)
/// - 4-6 -> Microsecond (4-6 fractional digits)
/// - 7-9 -> Nanosecond (7-9 fractional digits)
fn precision_to_time_unit(precision: u32) -> TimeUnit {
    match precision {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        7..=9 => TimeUnit::Nanosecond,
        _ => TimeUnit::Nanosecond, // Default to nanosecond for invalid precision
    }
}

fn data_field_to_arrow_field(field: &DataField) -> Result<Field> {
    let name = field.name();
    let data_type = field.data_type();
    let nullable = data_type.is_nullable();
    let arrow_data_type = data_type_to_arrow_data_type(data_type)?;
    Ok(Field::new(name, arrow_data_type, nullable))
}

/// Convert Paimon DataType to Arrow DataType.
fn data_type_to_arrow_data_type(data_type: &DataType) -> Result<ArrowDataType> {
    // Convert to Arrow DataType
    let arrow_data_type = match data_type {
        DataType::Boolean(_) => ArrowDataType::Boolean,
        DataType::TinyInt(_) => ArrowDataType::Int8,
        DataType::SmallInt(_) => ArrowDataType::Int16,
        DataType::Int(_) => ArrowDataType::Int32,
        DataType::BigInt(_) => ArrowDataType::Int64,
        DataType::Decimal(ty) => {
            let precision: u8 = ty
                .precision()
                .try_into()
                .map_err(|_| Error::DataTypeInvalid {
                    message: "incompatible precision for decimal type convert".to_string(),
                })?;
            let scale: i8 = ty.scale().try_into().map_err(|_| Error::DataTypeInvalid {
                message: "incompatible scale for decimal type convert".to_string(),
            })?;
            validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale).map_err(
                |_| Error::DataTypeInvalid {
                    message: "incompatible precision and scale for decimal type convert"
                        .to_string(),
                },
            )?;
            ArrowDataType::Decimal128(precision, scale)
        }
        DataType::Double(_) => ArrowDataType::Float64,
        DataType::Float(_) => ArrowDataType::Float32,
        DataType::Binary(_) => ArrowDataType::Binary,
        DataType::VarBinary(_) => ArrowDataType::Binary,
        DataType::Char(_) => ArrowDataType::Utf8,
        DataType::VarChar(_) => ArrowDataType::Utf8,
        DataType::Date(_) => ArrowDataType::Date32,
        DataType::Time(ty) => {
            let time_unit = precision_to_time_unit(ty.precision());
            // Arrow Time64 supports Microsecond and Nanosecond
            // For Second and Millisecond, we need to use Time32
            match time_unit {
                TimeUnit::Second => ArrowDataType::Time32(TimeUnit::Second),
                TimeUnit::Millisecond => ArrowDataType::Time32(TimeUnit::Millisecond),
                TimeUnit::Microsecond => ArrowDataType::Time64(TimeUnit::Microsecond),
                TimeUnit::Nanosecond => ArrowDataType::Time64(TimeUnit::Nanosecond),
            }
        }
        DataType::Timestamp(ty) => {
            let time_unit = precision_to_time_unit(ty.precision());
            ArrowDataType::Timestamp(time_unit, None)
        }
        DataType::LocalZonedTimestamp(ty) => {
            let time_unit = precision_to_time_unit(ty.precision());
            ArrowDataType::Timestamp(time_unit, Some(UTC_TIME_ZONE.into()))
        }
        DataType::Array(ty) => {
            let element_data_type = data_type_to_arrow_data_type(ty.element_type())?;
            let element_field = Field::new(
                "element",
                element_data_type,
                ty.element_type().is_nullable(),
            );
            ArrowDataType::List(Arc::new(element_field))
        }
        DataType::Map(ty) => {
            // Arrow Map requires a struct with "key" and "value" fields
            let key_data_type = data_type_to_arrow_data_type(ty.key_type())?;
            let value_data_type = data_type_to_arrow_data_type(ty.value_type())?;
            let key_field = Field::new("key", key_data_type, ty.key_type().is_nullable());
            let value_field = Field::new("value", value_data_type, ty.value_type().is_nullable());
            let entries_struct = ArrowDataType::Struct(Fields::from(vec![key_field, value_field]));
            ArrowDataType::Map(
                Arc::new(Field::new(DEFAULT_MAP_FIELD_NAME, entries_struct, false)),
                false,
            )
        }
        DataType::Multiset(ty) => {
            // Multiset is represented as a List in Arrow
            let element_data_type = data_type_to_arrow_data_type(ty.element_type())?;
            let element_field = Field::new(
                "element",
                element_data_type,
                ty.element_type().is_nullable(),
            );
            ArrowDataType::List(Arc::new(element_field))
        }
        DataType::Row(ty) => {
            let mut fields = Vec::with_capacity(ty.fields().len());
            for row_field in ty.fields() {
                let field_data_type = data_type_to_arrow_data_type(row_field.data_type())?;
                fields.push(Field::new(
                    row_field.name(),
                    field_data_type,
                    row_field.data_type().is_nullable(),
                ));
            }
            ArrowDataType::Struct(Fields::from(fields))
        }
    };

    Ok(arrow_data_type)
}
