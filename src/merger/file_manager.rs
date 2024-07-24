use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Decimal256Array, DurationNanosecondArray, FixedSizeBinaryArray, Float16Array, Float32Array,
    Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, IntervalYearMonthArray,
    LargeBinaryArray, NullArray, StringArray, Time32SecondArray, Time64NanosecondArray,
    TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct FileManager {
    file_paths: Vec<String>,
    schema: SchemaRef,
    output_file_path: String,
}

impl FileManager {
    pub async fn new(file_paths: Vec<String>, schema: SchemaRef, output_file_path: String) -> Self {
        Self {
            file_paths,
            schema,
            output_file_path,
        }
    }

    pub async fn merge_files(&self) {
        let combined_file = File::create_new(Path::new(&self.output_file_path)).unwrap();
        let arrow_schema = &self.schema;
        let owned_arrow_schema = arrow_schema.to_owned();
        let file_paths = &self.file_paths;
    }

    pub async fn write_record_batch_to_parquet(&self) {
        let combined_file = File::create_new(Path::new(&self.output_file_path)).unwrap();
        let arrow_schema = &self.schema;
        let owned_arrow_schema = arrow_schema.to_owned();
        let file_paths = &self.file_paths;

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
                Err(_) => Self::create_default_array(field.data_type(), record_batch.num_rows()),
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
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();

        let mut writer = ArrowWriter::try_new(file, schema.to_owned(), Some(props)).unwrap();
        writer.write(&record_batch).expect("Writing batch");

        writer.close().unwrap();
    }

    fn create_default_array(data_type: &DataType, num_rows: usize) -> ArrayRef {
        match data_type {
            DataType::Int64 => Arc::new(Int64Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Date32 => Arc::new(Date32Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Null => Arc::new(NullArray::new(num_rows)) as ArrayRef,
            DataType::Boolean => Arc::new(BooleanArray::from(vec![None; num_rows])) as ArrayRef,
            DataType::Int8 => Arc::new(Int8Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Int16 => Arc::new(Int16Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Int32 => Arc::new(Int32Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::UInt8 => Arc::new(UInt8Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::UInt16 => Arc::new(UInt16Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::UInt32 => Arc::new(UInt32Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::UInt64 => Arc::new(UInt64Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Float16 => Arc::new(Float16Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Float32 => Arc::new(Float32Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Float64 => Arc::new(Float64Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Timestamp(_, _) => {
                Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Date64 => Arc::new(Date64Array::from(vec![None; num_rows])) as ArrayRef,
            DataType::Time32(_) => {
                Arc::new(Time32SecondArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Time64(_) => {
                Arc::new(Time64NanosecondArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Duration(_) => {
                Arc::new(DurationNanosecondArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Interval(_) => {
                Arc::new(IntervalYearMonthArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Binary => Arc::new(BinaryArray::from(vec![None; num_rows])) as ArrayRef,
            DataType::FixedSizeBinary(_) => {
                Arc::new(FixedSizeBinaryArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::LargeBinary => {
                Arc::new(LargeBinaryArray::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Utf8 => {
                Arc::new(StringArray::from(vec![String::from(""); num_rows])) as ArrayRef
            }
            DataType::Decimal128(_, _) => {
                Arc::new(Decimal128Array::from(vec![None; num_rows])) as ArrayRef
            }
            DataType::Decimal256(_, _) => {
                Arc::new(Decimal256Array::from(vec![None; num_rows])) as ArrayRef
            }
            _ => unimplemented!("Type not supported"),
        }
    }
}

#[derive(Clone, Debug)]
pub enum FileType {
    Parquet,
    Csv,
    Json,
    Orc,
}

pub fn get_file_type(path: &Path) -> FileType {
    let ext = path.extension().and_then(OsStr::to_str);
    match ext {
        Some("parquet") => FileType::Parquet,
        Some("csv") => FileType::Csv,
        Some("json") => FileType::Json,
        Some("orc") => FileType::Orc,
        None => unimplemented!("file type not supported or there is no extension"),
        _ => panic!("some how the program ended up here, this should not be possible :("),
    }
}
