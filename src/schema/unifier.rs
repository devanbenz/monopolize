use std::collections::HashMap;
use crate::schema::extractor::{ExtractedSchema, SchemaExtractor};

pub struct SchemaUnifier {
    schema_list: HashMap<String, ExtractedSchema>
}

impl SchemaUnifier {
    pub fn new(schema_extractor: SchemaExtractor) -> Self {
        let mut schema_map: HashMap<String, ExtractedSchema> = HashMap::new();
        for schema in schema_extractor.schema {
            let name = &schema.name;
            if let Some(extracted_schema) = schema_map.get(name) {
                if schema.converted_type != extracted_schema.converted_type || schema.physical_type != extracted_schema.physical_type {
                    schema_map.insert(name.to_string(), schema);
                }
            } else {
                schema_map.insert(name.to_string(), schema);
            }
        }

        Self {
            schema_list: schema_map
        }
    }

    pub fn get_merged_schema(self) -> Vec<ExtractedSchema> {
        self.schema_list.into_values().collect::<Vec<ExtractedSchema>>()
    }
}
