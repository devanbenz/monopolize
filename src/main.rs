use crate::cli::handle_input;
use crate::error::Error;
use merger::file_manager::FileManager;
use crate::merger::schema_unifier::SchemaUnifier;

mod cli;
mod error;
mod querier;
mod merger;

fn main() -> Result<(), Error>{

    let (files, output) = handle_input()?;
    let arrow_unified_schema = SchemaUnifier::new(&files);
    let schema = arrow_unified_schema.merge_schema_from_files();
    let file_manager = FileManager::new(files, schema, output);
    file_manager.read_to_arrow_batches();

    Ok(())
}
