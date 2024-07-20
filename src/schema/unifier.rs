use std::collections::HashMap;
use crate::schema::extractor::{ExtractedSchema, SchemaExtractor};

pub struct SchemaUnifier {
    schema_list: HashMap<str, ExtractedSchema>
}

impl SchemaUnifier {
    pub fn new(schema_extractor: SchemaExtractor) {
        let mut schema_map: HashMap<String, ExtractedSchema> = HashMap::new();
        for schema in schema_extractor.schema {
            let name = &schema.name;
            if let Some(schema) = schema_map.get(name) {

            } else {
                schema_map.insert(name.to_string(), schema);
            }
        }
    }
}
