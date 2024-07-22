use crate::merger::arrow_schema_unifier::ArrowSchemaUnifier;
use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DictionaryArray, DurationNanosecondArray, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int16Array, Int32Array,
    Int64Array, Int8Array, IntervalYearMonthArray, LargeBinaryArray, LargeListArray,
    LargeStringArray, ListArray, MapArray, NullArray, StringArray, StructArray, Time32SecondArray,
    Time64NanosecondArray, TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array, UnionArray,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Clone)]
pub struct FileManager {
    unified_schema: ArrowSchemaUnifier,
}

impl FileManager {
    pub fn new(unified_schema: ArrowSchemaUnifier) -> Self {
        Self { unified_schema }
    }

    pub fn read_to_arrow_batches(self, file_path: String) {
        let combined_file = File::create_new(Path::new(&file_path)).unwrap();
        let arrow_schema = self.unified_schema.get_merged_schema();
        let owned_arrow_schema = arrow_schema.to_owned();
        let file_paths = self.unified_schema.get_file_paths();

        for file_path in file_paths {
            let file = File::open(Path::new(&file_path)).expect("could not open file");
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .expect("could not read record batches from parquet file");

            let reader = builder
                .build()
                .expect("could not build reader from parquet builder");
            for record_batch in reader {
                let merged_record_batch =
                    Self::convert_batch_to_schema(&owned_arrow_schema, record_batch.unwrap());
                Self::write_to_combined_schema_arrow_batch(
                    &combined_file,
                    &owned_arrow_schema,
                    merged_record_batch,
                );
            }
        }
    }

    fn convert_batch_to_schema(schema: &SchemaRef, record_batch: RecordBatch) -> RecordBatch {
        let mut columns: Vec<ArrayRef> = Vec::new();
        let schema_fields = schema.fields();

        for field in schema_fields {
            let array = match record_batch.schema().field_with_name(field.name()) {
                Ok(a) => {
                    let col = record_batch.column_by_name(a.name()).unwrap();
                    col.to_owned()
                }
                Err(_) => {
                    // Add default values if the field is missing
                    match field.data_type() {
                        DataType::Int64 => {
                            Arc::new(Int64Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Date32 => {
                            Arc::new(Date32Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Null => {
                            Arc::new(NullArray::new(record_batch.num_rows())) as ArrayRef
                        }
                        DataType::Boolean => {
                            Arc::new(BooleanArray::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Int8 => {
                            Arc::new(Int8Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Int16 => {
                            Arc::new(Int16Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Int32 => {
                            Arc::new(Int32Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::UInt8 => {
                            Arc::new(UInt8Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::UInt16 => {
                            Arc::new(UInt16Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::UInt32 => {
                            Arc::new(UInt32Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::UInt64 => {
                            Arc::new(UInt64Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Float16 => {
                            Arc::new(Float16Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Float32 => {
                            Arc::new(Float32Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Float64 => {
                            Arc::new(Float64Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Timestamp(_, _) => {
                            Arc::new(TimestampNanosecondArray::from(vec![
                                None;
                                record_batch.num_rows()
                            ])) as ArrayRef
                        } // Assuming nanoseconds precision
                        DataType::Date64 => {
                            Arc::new(Date64Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Time32(_) => {
                            Arc::new(Time32SecondArray::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        } // Assuming seconds
                        DataType::Time64(_) => {
                            Arc::new(Time64NanosecondArray::from(vec![
                                None;
                                record_batch.num_rows()
                            ])) as ArrayRef
                        } // Assuming nanoseconds
                        DataType::Duration(_) => {
                            Arc::new(DurationNanosecondArray::from(vec![
                                None;
                                record_batch.num_rows()
                            ])) as ArrayRef
                        } // Assuming nanoseconds
                        DataType::Interval(_) => {
                            Arc::new(IntervalYearMonthArray::from(vec![
                                None;
                                record_batch.num_rows()
                            ])) as ArrayRef
                        } // Assuming year-month interval
                        DataType::Binary => {
                            Arc::new(BinaryArray::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::FixedSizeBinary(_) => {
                            Arc::new(FixedSizeBinaryArray::from(vec![
                                None;
                                record_batch.num_rows()
                            ])) as ArrayRef
                        }
                        DataType::LargeBinary => {
                            Arc::new(LargeBinaryArray::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Utf8 => Arc::new(StringArray::from(vec![
                            String::from("");
                            record_batch.num_rows()
                        ])) as ArrayRef,
                        // DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec![None; record_batch.num_rows()])) as ArrayRef,
                        // DataType::List(a) => Arc::new(ListArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder, may need custom handling
                        // DataType::FixedSizeList(_, size) => Arc::new(FixedSizeListArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder
                        // DataType::LargeList(_) => Arc::new(LargeListArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder
                        // DataType::Struct(_) => Arc::new(StructArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder, needs custom handling
                        // DataType::Union(_, _) => Arc::new(UnionArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder, needs custom handling
                        // DataType::Dictionary(_, _) => Arc::new(DictionaryArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder, needs custom handling
                        DataType::Decimal128(_, _) => {
                            Arc::new(Decimal128Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        DataType::Decimal256(_, _) => {
                            Arc::new(Decimal256Array::from(vec![None; record_batch.num_rows()]))
                                as ArrayRef
                        }
                        // DataType::Map(_, _) => Arc::new(MapArray::from(vec![None; record_batch.num_rows()])) as ArrayRef, // Placeholder, needs custom handling
                        _ => unimplemented!("Type not supported"),
                    }
                }
            };
            columns.push(array);
        }

        RecordBatch::try_new(schema.clone(), columns).unwrap()
    }

    fn write_to_combined_schema_arrow_batch(
        file: &File,
        schema: &SchemaRef,
        record_batch: RecordBatch,
    ) {
        println!("writing record batch: {:#?}", record_batch.num_rows());

        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.to_owned(), Some(props)).unwrap();
        writer.write(&record_batch).expect("Writing batch");

        writer.close().unwrap();
    }
}
