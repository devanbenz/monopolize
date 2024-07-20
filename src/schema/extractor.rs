use std::fs::File;
use std::path::Path;
use parquet::basic::{ConvertedType, Type as PhysicalType};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;

#[derive(Debug)]
pub struct SchemaExtractor {
    pub schema: Vec<ExtractedSchema>
}

#[derive(Debug)]
pub struct ExtractedSchema {
    pub name: String,
    pub physical_type: PhysicalType,
    pub converted_type: ConvertedType
}


impl SchemaExtractor {
    pub fn new(path: &str) -> Self {
        let path = Path::new(path);
        let mut schema: Vec<ExtractedSchema> = vec![];

        if let Ok(file) = File::open(path) {
            let reader = SerializedFileReader::new(file).unwrap();
            let parquet_metadata = reader.metadata().file_metadata();

            for val in parquet_metadata.schema().get_fields() {
                schema.push(ExtractedSchema {
                    name : val.name().parse().unwrap(),
                    physical_type: val.get_physical_type(),
                    converted_type: val.get_basic_info().converted_type()
                });
            }
        }

        Self {
            schema
        }
    }
}

