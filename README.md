# DE-Azure-project

## Simple ETL end-to-end project

This project is done in azure cloud platform, majorily used tools are,
  1) Azure storage account (data lake)
  2) Azure Data Factory
  3) Databricks
  4) Access connector for Databricks
  5) Azure sql database and server

#### Major feature are
1) Backfilling (If we provide from_date in loop_input file for each table it will back fill from that date)
2) Can process multiple Dimension (Tables)
3) Github intgration (Can directly push the file into git repo within clicks with all changes (commits)
4) Deletes empty files automatically after ingestion process
5) Integration of for_each activities for automating for each individual table

   
## Azure Data Factory
### Incremental_ingestion pipeline

Here I've created simple incremental_ingestion Pipeline and Loop_increamental_ingestion pipeline, where the incremental_ingestion pipeline fetches the data (For all Tables) from sql database and it stores in bronze layer, but it has to be run for all tables so Loop_incremen
