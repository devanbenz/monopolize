use crate::modules::file_manager::{get_file_type, FileType};
use arrow::datatypes::{Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;
use std::ops::Deref;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct SchemaUnifier {
    file_paths: Vec<String>,
}

impl SchemaUnifier {
    pub fn new(file_paths: Vec<String>) -> Self {
        Self { file_paths }
    }

    pub fn merge_schema_from_files(&self) -> SchemaRef {
        let mut merged_arrow_fields: Vec<Field> = vec![];

        for file_path in &self.file_paths {
            let file_path = Path::new(file_path);
            let file = File::open(file_path).expect("could not open file for reading");

            match get_file_type(file_path) {
                FileType::Parquet => Self::parquet_to_arrow_schema(&mut merged_arrow_fields, file),
                FileType::Csv => Self::csv_to_arrow_schema(&mut merged_arrow_fields, file),
                FileType::Json => Self::json_to_arrow_schema(&mut merged_arrow_fields, file),
                FileType::Orc => Self::orc_to_arrow_schema(&mut merged_arrow_fields, file),
            }
        }

        SchemaRef::new(Schema::new(merged_arrow_fields))
    }

    pub fn get_file_paths(self) -> Vec<String> {
        self.file_paths
    }

    fn parquet_to_arrow_schema(arrow_fields: &mut Vec<Field>, file: File) {
        let arrow_schema = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let fields = arrow_schema.schema();

        for field in fields.fields() {
            let cloned_field = field.clone().deref().to_owned();
            arrow_fields.push(cloned_field);
        }
    }

    fn csv_to_arrow_schema(arrow_fields: &mut Vec<Field>, file: File) {}

    fn json_to_arrow_schema(arrow_fields: &mut Vec<Field>, file: File) {}

    fn orc_to_arrow_schema(arrow_fields: &mut Vec<Field>, file: File) {}
}
