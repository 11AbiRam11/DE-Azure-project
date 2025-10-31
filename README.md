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
![Alt Text](https://github.com/11AbiRam11/DE-Azure-project/blob/main/assets/incremental_ingestion.png)

This pipeline is designed to **extract data from an Azure SQL Database** and load it into the **Bronze layer of the Data Lake** in **Parquet format**.  
It incorporates **Change Data Capture (CDC)** logic to ensure **incremental data ingestion** and **efficient updates**.

### Configuration Files

**`cdc.json`**  
- Maintains the **timestamp of the last successful data load**.  
- Automatically updated after each pipeline run to reflect the latest modification date.

**`empty.json`**  
- Used to **reset or override the CDC configuration**, enabling a **full data refresh** from the SQL source when required.

### Post-Processing & Optimization

After each execution, the pipeline performs **housekeeping operations**, such as:  
- Removing **empty files** from the Bronze layer.  
- Optimizing **storage** and maintaining **data integrity**.

### Limitations

- Currently, the pipeline supports **only a single table per execution**.  
- For scenarios involving **multiple tables**, the pipeline must be executed separately for each table, which may impact efficiency in large-scale use cases.

----

## Loop_incremental_ingestion pipeline (Optimized Pipeline)
![Alt Text](https://github.com/11AbiRam11/DE-Azure-project/blob/main/assets/loop_ingestion.png)


This pipeline is an **enhanced version of the previous ingestion pipeline**, designed to handle **multiple tables in a single execution**.  
By configuring the `loop_input` file, which contains an **array of dictionaries** representing the tables to be processed, the pipeline can automatically iterate through all specified tables using a **for_each activity**.  

Upon successful completion of the pipeline, a **POST request** is sent to an external endpoint, triggering an **email notification to the development team** to inform them of the execution status.  
This automation improves efficiency, reduces manual intervention, and ensures timely updates for the engineering team.


### Automated Email Notifications (Azure's Logic Apps)
![Alt Text](https://github.com/11AbiRam11/DE-Azure-project/blob/main/assets/Logic_app.png)



The pipeline includes a **robust notification feature** that automatically sends emails based on the pipeline status: **Success, Failure, or Skipped**.  
This is implemented using **Azure Logic Apps**, which handles the email delivery to the development team.

Once the Logic App resource is created, it requires authentication with a valid email account. After configuration, the Logic App provides an **external endpoint** that the pipeline calls to trigger notifications.  
Each notification includes **key details** such as the **pipeline name** and **pipeline ID**, enabling the development team to quickly identify the status and context of the pipeline run.

-----

### Data Transformation in Databricks

Data transformation is performed using **Databricks notebooks (`.ipynb`)** along with **custom utility modules**.  
The pipeline leverages **PySpark** and modular Python functions to perform operations such as:  

- Data cleaning and validation  
- Type casting and schema enforcement  
- Aggregations and calculations  
- Writing transformed data to the **Silver layer** in delta format  

This approach ensures **scalable, reusable, and maintainable transformation logic** across different datasets and tables.

### Metadata-Driven Queries

To access Parquet files stored in the **Azure Storage Account**, an **access connector** must be configured.  
This process provides a **`resource_id`**, which is then used in **Databricks** to establish a secure connection.

In Databricks, a **new credential** is created by providing the `resource_id`, enabling seamless access to the storage account.  
This connection allows the pipeline to efficiently read and write data between the **SQL Database** and **Databricks**, ensuring secure and reliable data transfers.

The pipeline leverages **Jinja2 templates** to make SQL queries **dynamic and metadata-driven**.  
By parameterizing queries using Jinja2, the pipeline can:  

- Adapt to different tables and columns without modifying the code  
- Support multi-table processing with reusable query templates  
- Simplify maintenance and reduce hardcoding

#### Example Parameter Configuration

```python
parameters = [
    {
        "table": "spotify_catalog.silver.factstream",
        "alias": "factstream",
        "cols": "factstream.stream_id, factstream.listen_duration"
    },
    {
        "table": "spotify_catalog.silver.dimuser",
        "alias": "dimuser",
        "cols": "dimuser.user_id, dimuser.user_name",
        "condition": "factstream.user_id = dimuser.user_id"
    },
    {
        "table": "spotify_catalog.silver.dimtrack",
        "alias": "dimtrack",
        "cols": "dimtrack.track_id, dimtrack.track_name",
        "condition": "factstream.track_id = dimtrack.track_id"
    }
]
This approach enhances **flexibility, scalability, and automation** in the ETL process.
```
And the Templated query looks like this 
```sql
query_text = """
       SELECT 
            {% for param in parameters %}
                {{ param.cols }}
                    {% if not loop.last %}
                        ,
                    {% endif %}
            {% endfor %}
    FROM
            {% for param in parameters %}
                {% if loop.first %}
                    {{ param.table }} AS {{ param.alias }}
                {% endif %}
            {% endfor %}
    {% for param in parameters %}
                {% if not loop.first %}
                LEFT JOIN 
                    {{ param['table'] }} AS {{ param['alias'] }}
                ON 
                    {{ param['condition']}}
                {% endif %}
            {% endfor %}
"""
```
Final Query looks like, without any effort or creating sql query manually again and again
```python
  SELECT
          factstream.stream_id, factstream.listen_duration,
          dimuser.user_id, dimuser.user_name,            
          dimtrack.track_id, dimtrack.track_name   
  FROM
        spotify_catalog.silver.factstream AS factstream
  LEFT JOIN 
        spotify_catalog.silver.dimuser AS dimuser
  ON 
        factstream.user_id = dimuser.user_id
  LEFT JOIN 
        spotify_catalog.silver.dimtrack AS dimtrack
  ON 
        factstream.track_id = dimtrack.track_id          
```
----

### Gold Layer (Final Layer)

The **Gold Layer** represents the **final, analytics-ready data** in the pipeline.  
It is built on top of the **Silver Layer**, which contains **cleaned and transformed data** stored in **Delta format**.

#### Real-Time Data Processing
- The pipeline uses **PySpark Structured Streaming** functions:  
  - `spark.readStream()` to **ingest only new records** from the Silver Layer.  
  - `spark.writeStream()` to **write incremental updates** to the Gold Layer.  
- This approach ensures **real-time data ingestion and processing**, avoiding the overhead of reading/writing the entire dataset repeatedly.

#### Slowly Changing Dimension (SCD) Implementation
- After staging the Silver Layer data, **Slowly Changing Dimension (SCD) tables** are created in the Gold Layer to track historical changes in key dimension tables.  
- This enables **accurate analytics and reporting**, preserving the history of changes while keeping the current dataset up-to-date.

By leveraging **Delta Lake** and **structured streaming**, the Gold Layer ensures:  
- Efficient **incremental processing**  
- **Historical tracking** of changes  
- **Analytics-ready data** for downstream consumption (e.g., BI dashboards, reporting tools)

A dedicated **Databricks pipeline** has been created to handle **Slowly Changing Dimensions (SCD)**, ensuring that historical changes in dimension tables are properly tracked and managed.  

The deployment process is structured as follows:

1. **Pipeline Creation**  
    - Develop Databricks notebooks and utility modules to implement SCD logic.  
    - Handle insertions, updates, and historical tracking in dimension tables.

2. **Bundling for Deployment**  
    - All notebooks, configuration files, and environment dependencies are **bundled together** using Databricks’ bundle mechanism.  
    - This ensures **consistent and reproducible deployments** across environments.

3. **Deployment Commands**
    - **Development Environment:**  
       ```bash
       databricks bundle deploy --target dev
       ```  
    - **Production Environment:**  
       ```bash
       databricks bundle deploy --target production
       ```  

4. **Key Benefits**  
    - Simplifies **multi-environment deployments** (Dev, QA, Production).  
    - Ensures **version control and environment consistency**.  
    - Facilitates **automation** and reduces manual intervention during deployment.


