use crate::errors::error::Error;
use crate::input::cli::handle_input;
use crate::schema::extractor::SchemaExtractor;
use crate::schema::manager::ArrowSchemaManager;
use crate::schema::unifier::SchemaUnifier;

mod input;
mod errors;
mod schema;
mod file_manager;

fn main() -> Result<(), Error>{
    let mut extractor = SchemaExtractor::new();

    let files = handle_input()?;
    for file in files {
        extractor.add_schema_to_extractor(&file);
    }

    let unified_schema = SchemaUnifier::new(extractor);
    let unified_schema = ArrowSchemaManager::new(unified_schema); 
    println!("{:#?}", unified_schema);

    Ok(())
}
