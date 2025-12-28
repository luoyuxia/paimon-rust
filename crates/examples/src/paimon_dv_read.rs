use futures_util::TryStreamExt;
use paimon::catalog::{Catalog, FileSystemCatalog, Identifier};
use paimon::io::FileIOBuilder;

#[tokio::main]
async fn main() -> paimon::Result<()> {

    let warehouse_path = "/Users/yuxia/Projects/rust-projects/paimon-rust/crates/paimon/tests/warehouse";
    let file_io = FileIOBuilder::new("file")
        .build()?;
    let catalog = FileSystemCatalog::new(
        warehouse_path,
        file_io
    );

    let table_id = Identifier::new("default", "T");

    let table = catalog.get_table(
        &table_id
    ).await?;


    let scan =
        table.scan()
            .snapshot_id(5)
            .build().await?;

    let arrow_batch = scan.to_arrow().await?;

    let batches: Vec<_> = arrow_batch.try_collect().await?;

    for batch in batches {
        println!("{:?}", batch);
    }

    Ok(())
}