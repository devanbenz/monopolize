use datafusion::prelude::{ParquetReadOptions, SessionContext};

pub async fn query_parquet_file(
    sql_query: &str,
    table_registration: &str,
    table_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        table_registration,
        table_path,
        ParquetReadOptions::default(),
    )
    .await?;
    let df = ctx.sql(sql_query).await?;

    df.show().await?;
    Ok(())
}
