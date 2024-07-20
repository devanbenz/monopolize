use crate::errors::error::Error;
use crate::input::cli::handle_input;
use crate::schema::extractor::SchemaExtractor;

mod input;
mod errors;
mod schema;

fn main() -> Result<(), Error>{
    let files = handle_input()?;
    for file in files {
        let a = SchemaExtractor::new(&file);
        println!("{:#?}", a);
    }

    Ok(())
}
