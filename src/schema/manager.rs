use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::basic::{ConvertedType, Type};
use crate::schema::unifier::SchemaUnifier;

#[derive(Debug)]
pub struct ArrowSchemaManager {
    pub schema: Schema
}

impl ArrowSchemaManager {
    pub fn new(unified_schema: SchemaUnifier) -> Self {
        let mut fields: Vec<Field> = vec![];
        for schema in unified_schema.get_merged_schema() {
            match schema.physical_type {
                Type::BOOLEAN => {
                    fields.push(Field::new(schema.name, DataType::Boolean, false))
                }
                Type::INT32 => {
                    fields.push(Field::new(schema.name, DataType::Int32, false))
                }
                Type::INT64 => {
                    fields.push(Field::new(schema.name, DataType::Int64, false))
                }
                Type::INT96 => {
                    fields.push(Field::new(schema.name, DataType::Int64, false))
                }
                Type::FLOAT => {
                    fields.push(Field::new(schema.name, DataType::Float32, false))
                }
                Type::DOUBLE => {
                    fields.push(Field::new(schema.name, DataType::Float64, false))
                }
                Type::BYTE_ARRAY => {
                    fields.push(Field::new(schema.name, DataType::Utf8, false))
                }
                Type::FIXED_LEN_BYTE_ARRAY => {
                    fields.push(Field::new(schema.name, DataType::LargeUtf8, false))
                }
            }
        }

        Self {
            schema: Schema::new(fields)
        }
    }

    pub fn new_from_converted(unified_schema: SchemaUnifier) -> Self {
        let mut fields: Vec<Field> = vec![];
        for schema in unified_schema.get_merged_schema() {
            match schema.converted_type {
                ConvertedType::NONE => {
                    fields.push(Field::new(schema.name, DataType::Null, false))
                }
                ConvertedType::UTF8 => {
                    fields.push(Field::new(schema.name, DataType::Utf8, false))
                }
                ConvertedType::MAP => todo!(),
                ConvertedType::MAP_KEY_VALUE => todo!(),
                ConvertedType::LIST => todo!(),
                ConvertedType::ENUM => todo!(),
                ConvertedType::DECIMAL => {
                    fields.push(Field::new(schema.name, DataType::Float64, false))
                }
                ConvertedType::DATE => {
                    fields.push(Field::new(schema.name, DataType::Date64, false))
                }
                ConvertedType::TIME_MILLIS => {
                    fields.push(Field::new(schema.name, DataType::Time64(TimeUnit::Millisecond), false))
                }
                ConvertedType::TIME_MICROS => {
                    fields.push(Field::new(schema.name, DataType::Time64(TimeUnit::Microsecond), false))
                }
                ConvertedType::TIMESTAMP_MILLIS => {
                    fields.push(Field::new(schema.name, DataType::Timestamp(TimeUnit::Millisecond, None), false))
                }
                ConvertedType::TIMESTAMP_MICROS => {
                    fields.push(Field::new(schema.name, DataType::Timestamp(TimeUnit::Microsecond, None), false))
                }
                ConvertedType::UINT_8 => {
                    fields.push(Field::new(schema.name, DataType::UInt8, false))
                }
                ConvertedType::UINT_16 => {
                    fields.push(Field::new(schema.name, DataType::UInt16, false))
                }
                ConvertedType::UINT_32 => {
                    fields.push(Field::new(schema.name, DataType::UInt32, false))
                }
                ConvertedType::UINT_64 => {
                    fields.push(Field::new(schema.name, DataType::UInt64, false))
                }
                ConvertedType::INT_8 => {
                    fields.push(Field::new(schema.name, DataType::Int8, false))
                }
                ConvertedType::INT_16 => {
                    fields.push(Field::new(schema.name, DataType::Int16, false))
                }
                ConvertedType::INT_32 => {
                    fields.push(Field::new(schema.name, DataType::Int32, false))
                }
                ConvertedType::INT_64 => {
                    fields.push(Field::new(schema.name, DataType::Int64, false))
                }
                ConvertedType::JSON => {
                    fields.push(Field::new(schema.name, DataType::BinaryView, false))
                }
                ConvertedType::BSON => todo!(),
                ConvertedType::INTERVAL => todo!()
            }
        }

        Self {
            schema: Schema::new(fields)
        }
    }
}
