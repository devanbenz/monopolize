use crate::cli::handle_input;
use crate::error::Error;
use crate::merger::arrow_schema_unifier::ArrowSchemaUnifier;
use crate::merger::file_manager::FileManager;

mod cli;
mod error;
mod merger;
mod querier;
mod modules;

fn main() -> Result<(), Error>{

    let (files, output) = handle_input()?;
    let arrow_unified_schema = ArrowSchemaUnifier::try_from_files(files);
    let file_manager = FileManager::new(arrow_unified_schema);
    file_manager.read_to_arrow_batches(output);

    Ok(())
}
