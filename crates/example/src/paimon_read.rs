mod paimon_dv_read;
mod paimon_datafusion_read;

use futures_util::StreamExt;
use paimon::catalog::Identifier;
use paimon::io::FileIOBuilder;
use paimon::{Catalog, FileSystemCatalog};

use paimon::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let warehouse_path = "/Users/yuxia/Desktop/paimon-warehouse";
    let file_io = FileIOBuilder::new("file").build()?;
    let catalog = FileSystemCatalog::new(file_io, warehouse_path);

    let table_id = Identifier::new("default", "T");

    let table = catalog.get_table(&table_id).await?;

    let plan = table.new_read_builder().new_scan().plan().await?;

    let mut arow_batch_stream = table
        .new_read_builder()
        .new_read()
        .to_arrow(plan.splits())?;

    while let Some(Ok(record_batch)) = arow_batch_stream.next().await {
        println!("RecordBatch: {record_batch:#?}");
    }

    Ok(())
}
