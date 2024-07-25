use crate::error::Error;
use crate::querier::query_engine::query_parquet_file;

mod api;
mod cli;
mod error;
mod merger;
mod querier;

#[tokio::main]
async fn main() -> Result<(), Error> {
    query_parquet_file(
        "SELECT * FROM parquet_file WHERE \"Sunshine\" IS NOT NULL LIMIT 10",
        "parquet_file",
        "/Users/devan/Documents/Projects/monopolize/out.parquet",
    )
    .await
    .expect("could not query file");

    Ok(())
}
