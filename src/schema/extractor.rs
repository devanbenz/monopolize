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
    pub fn new() -> Self {
        let schema: Vec<ExtractedSchema> = vec![];

        Self {
            schema
        }
    }

    pub fn add_schema_to_extractor(&mut self, path: &str) -> &mut SchemaExtractor {
        let path = Path::new(path);

        if let Ok(file) = File::open(path) {
            let reader = SerializedFileReader::new(file).unwrap();
            let parquet_metadata = reader.metadata().file_metadata();

            for val in parquet_metadata.schema().get_fields() {
                self.schema.push(ExtractedSchema {
                    name : val.name().parse().unwrap(),
                    physical_type: val.get_physical_type(),
                    converted_type: val.get_basic_info().converted_type()
                });
            }
        }

        self
    }
}

