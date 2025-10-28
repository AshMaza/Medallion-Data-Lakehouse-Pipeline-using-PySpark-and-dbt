#### Medallion-Data-Lakehouse-Pipeline-using-PySpark-and-dbt

A production-style **Lakehouse** pipeline implementing the **Medallion architecture**—**Bronze (raw streaming ingest in Delta)** and **Silver (curated CDC/upserts) in PySpark**, with **Gold (dim/fact models, snapshots, incremental models) in dbt**—running on **Databricks**.

> 🔎 Dataset theme: ride-hailing domain with core entities: `customers`, `drivers`, `payments`, `trips`, `locations`, `vehicles`.  
> 🚫 Note: The project intentionally **does not** mention the original provider name.

## 📸 Screenshots 

> Replace the placeholder image links below by dragging your images into your repo and updating the paths.

1. **Workspace & Catalogs** – 
![Workspace Screenshot 1](images/Pasted%20image%2020251028175652.png)
![Workspace Screenshot 2](images/Pasted%20image%2020251028175804.png)

2. **Bronze Streaming Write Success** –
![Bronze Streaming](images/Pasted%20image%2020251028175847.png)
    
3. **Silver Tables After Upsert** – 
![Silver Tables](images/Pasted%20image%2020251028175924.png)
    
4. **dbt Cloud Models & Sources** –  
![dbt Models](images/Pasted%20image%2020251028180107.png)
    
5. **dbt Run Success + Gold Schema** –
![dbt Run 1](images/Pasted%20image%2020251028180301.png)
![dbt Run 2](images/Pasted%20image%2020251028180149.png)

6. **Lineage (optional)** – 
![Lineage View](images/Pasted%20image%2020251028180404.png)
    
    ### **🧱 Architecture**
		
	source_data (files) ──► BRONZE (Delta, streaming append)
                        │
                        └─► SILVER (Delta, CDC merge/upsert, de-dup, audit cols)
                                  │
                                  └─► GOLD (dbt models: incremental fact + SCD snapshots)

- **Bronze**: schema-on-read, streaming ingest from `/Volumes/<catalog>/source/source_data/<entity>/` to Delta.
    
- **Silver**: standardized, **de-duplicated**, **CDC aware** (merge on keys, latest timestamp wins).
    
- **Gold**: **dbt** incremental model for `trips` and **SCD** snapshots for dims & fact.

## 🧰 Tech Stack

- **Databricks** (Unity Catalog, Delta Lake, DBFS Volumes)
    
- **PySpark** (Structured Streaming, Window functions)
    
- **Delta Lake** (MERGE, schema evolution handling)
    
- **dbt Cloud** + `dbt-databricks` (models, sources, snapshots)
    
- **GitHub** (version control)

### 📂 Repository Structure

Medallion-Data-Lakehouse-Pipeline-using-PySpark-and-dbt/
├─ notebooks/                         # existing PySpark notebooks
│  ├─ bronze_ingestion.py
│  └─ silver_transformation.py
├─ dbt/
│  ├─ dbt_project.yml
│  ├─ models/
│  │  ├─ silver/
│  │  │  └─ trips.sql
│  │  └─ gold/                        # (populated by snapshots & refs)
│  ├─ macros/
│  │  └─ generate_schema_name.sql
│  ├─ snapshots/
│  │  ├─ SCDs.yml
│  │  └─ fact.yml
│  └─ sources.yml
├─ images/                            # ← put screenshots here
└─ README.md

## 🗂️ Catalogs & Schemas

- **Catalog**: `pysparkdbt`
    
- **Schemas**: `source`, `bronze`, `silver`, `gold`

## 🟤 Bronze Layer – _Raw Ingestion_

📍 **Where:** Databricks Notebook → `bronze_ingestion`

**What happens here:**

- A Spark job loops through all entities (`customers`, `trips`, etc.) and reads CSV files from `/Volumes/pysparkdbt/source/source_data/<entity>/`.
    
- The data is read as **structured streaming** with schema inference.
    
- Each entity is written to Delta tables under `pysparkdbt.bronze.<entity>` with checkpointing for reliability.
    
- This enables _incremental and append-only ingestion_ from the source folder.

![Bronze Layer](images/Pasted%20image%2020251028175847.png)



## ⚪ Silver Layer – _Cleansed & Curated_

📍 **Where:** Databricks Notebook → `silver_transformation`

**What happens here:**

- Applies **data quality and CDC (Change Data Capture)** logic on top of Bronze tables.
    
- Uses a reusable `transformations` class that:
    
    - **Deduplicates** data using `row_number()` over key columns.
        
    - **Adds audit timestamps** (`process_timestamp`).
        
    - **Performs upserts (MERGE)** into Delta tables using CDC fields like `last_updated_timestamp`.
        
- All entities except `trips` are processed here.
    
- Final curated Delta tables are stored in `pysparkdbt.silver.<entity>`.

![Silver Layer](images/Pasted%20image%2020251028175924.png)

## 🟡 Gold Layer – _Business Models & Snapshots_

📍 **Where:** dbt Cloud Project → `pysparkdbt_project`

**What happens here:**

- **dbt** connects to Databricks using the `dbt-databricks` adapter.
    
- Gold schema holds business-ready tables built from silver and bronze data.
    
- The dbt project includes:
    
    - **Sources:** Configured for bronze & silver schemas.
        
    - **Models:** Incremental model for `trips` table with `unique_key = trip_id`.
        
    - **Snapshots:** Slowly Changing Dimensions (SCD Type-2) for customers, drivers, vehicles, locations, and payments.
        
    - **Fact Table:** Snapshot of trips as `FactTrips`.

![Gold Layer 1](images/Pasted%20image%2020251028181500.png)
![Gold Layer 2](images/Pasted%20image%2020251028181429.png)

## ▶️ How to Run

1. Upload all raw CSVs under  
    `/Volumes/pysparkdbt/source/source_data/<entity>/`.
    
2. Run the **Bronze ingestion notebook** → creates streaming Delta tables.
    
3. Run the **Silver transformation notebook** → cleans and upserts curated data.
    
4. In dbt Cloud → run:
    
    `dbt run dbt snapshot`
    
5. Verify that new **Gold schema** tables are created in Databricks.
    

---

## 🧩 Highlights

- **Medallion Design:** Clear separation of ingestion, curation, and modeling.
    
- **Streaming ingestion:** Reliable and schema-aware.
    
- **CDC/Upsert handling:** Automated merge logic with deduplication.
    
- **dbt Integration:** Incremental loads and SCD snapshots for analytics.
    
- **Version control:** Fully managed via GitHub.
    

---

## 🧠 Key Learnings

- Implementing Delta Lake + dbt simplifies incremental transformations.
    
- Unified **PySpark + SQL** pipeline design scales easily for new datasets.
    
- dbt snapshots provide a reliable mechanism for time-variant dimensional models.
