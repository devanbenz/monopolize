use std::collections::HashMap; 
use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use arrow::datatypes::{Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[derive(Debug, Clone)]
pub struct ArrowSchemaUnifier {
    schema_list: SchemaRef,
    file_paths: Vec<String>
}

impl ArrowSchemaUnifier {
    pub fn try_from_files(file_paths: Vec<String>) -> Self {
        let mut schema_map: HashMap<String, Field> = HashMap::new();
        for file in file_paths.clone() {
            let file = File::open(Path::new(&file)).unwrap();
            let arrow_schema = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let fields = arrow_schema.schema();
            
            for field in fields.fields() {
                let cloned_field = field.clone().deref().to_owned();
                
                if let Some(_extracted_schema) = schema_map.get(field.name()) {
                } else {
                    schema_map.insert(field.name().to_string(), cloned_field);
                }
            }
        }
        let fields = schema_map.into_values().collect::<Vec<Field>>();
        
        Self {
            schema_list: SchemaRef::new(Schema::new(fields)),
            file_paths 
        }
    }

    pub fn get_merged_schema(&self) -> &SchemaRef {
        &self.schema_list
    }
    
    pub fn get_file_paths(self) -> Vec<String> {
        self.file_paths
    }
}
