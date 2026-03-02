

## Table of Contents
1. [What is Microsoft Fabric?](#1-what-is-microsoft-fabric)
2. [Tenant, Capacity & Workspace Hierarchy](#2-tenant-capacity--workspace-hierarchy)
3. [Free Account Setup](#3-free-account-setup)
4. [OneLake — The Foundation](#4-onelake--the-foundation)
5. [Delta Parquet File Format](#5-delta-parquet-file-format)
6. [Storage Solutions: Warehouse vs Lakehouse vs KQL](#6-storage-solutions-warehouse-vs-lakehouse-vs-kql)
7. [Data Ingestion Methods](#7-data-ingestion-methods)
8. [Fabric Data Factory — Deep Dive](#8-fabric-data-factory--deep-dive)
9. [Real-Time Scenarios in Data Factory](#9-real-time-scenarios-in-data-factory)
10. [Dataflow Gen2](#10-dataflow-gen2)
11. [Fabric Data Engineering (PySpark)](#11-fabric-data-engineering-pyspark)
12. [Fabric Shortcuts](#12-fabric-shortcuts)
13. [Medallion Architecture in Lakehouse](#13-medallion-architecture-in-lakehouse)
14. [Fabric Data Warehouse](#14-fabric-data-warehouse)
15. [Dimensional Data Modeling & SCD](#15-dimensional-data-modeling--scd)
16. [Fabric Data Science](#16-fabric-data-science)
17. [Data Governance & Lineage](#17-data-governance--lineage)
18. [CI/CD with Deployment Pipelines](#18-cicd-with-deployment-pipelines)
19. [Direct Lake vs Direct Query vs Import Mode](#19-direct-lake-vs-direct-query-vs-import-mode)
20. [Projects & Next Steps](#20-projects--next-steps)

---

## 1. What is Microsoft Fabric?

Microsoft Fabric is a **unified SaaS (Software as a Service) platform** that consolidates all major data services under a single environment — eliminating the need to integrate separate Azure tools.

### Services Unified Under Fabric
| Service in Fabric | Azure Equivalent |
|---|---|
| Data Factory | Azure Data Factory (ADF) |
| Data Engineering | Azure Databricks / Spark |
| Data Warehouse | Azure Synapse Analytics (SQL Pool) |
| Data Science | Azure Machine Learning |
| Real-Time Analytics | Azure Stream Analytics / Event Hubs |
| Power BI | Power BI (now embedded natively) |
| Databases | Azure SQL Databases |
| Data Governance | Microsoft Purview |

### Why Fabric Over Traditional Azure?
- **No integration overhead** — services communicate natively
- **No networking configurations** between resources
- **Single storage layer (OneLake)** — one copy of data for all services
- **SaaS model** — Microsoft manages all infrastructure

```
Traditional Azure Stack:
ADF → ADLS Gen2 → Synapse → Power BI  (4 separate services, 3 integrations)

Microsoft Fabric:
Data Factory + Lakehouse + Warehouse + Power BI  (1 unified platform, 0 manual integrations)
```

---

## 2. Tenant, Capacity & Workspace Hierarchy

```
┌─────────────────────────────────────────────┐
│                  TENANT                      │
│  (Your organization / email domain)          │
│  ┌─────────────────────────────────────────┐ │
│  │           CAPACITY (F64 SKU)            │ │
│  │  Pool of compute: CPU, RAM, Storage     │ │
│  │  ┌─────────────┐  ┌─────────────────┐  │ │
│  │  │  Workspace 1│  │  Workspace 2    │  │ │
│  │  │ (Container) │  │  (Container)    │  │ │
│  │  └─────────────┘  └─────────────────┘  │ │
│  └─────────────────────────────────────────┘ │
└─────────────────────────────────────────────┘
```

### Tenant
- Created automatically when you set up an Azure or Microsoft 365 account
- Your email domain acts as the DNS identifier
- Owns all subscriptions, users, and capacities

### Capacity (SKUs)
| SKU | Capacity Units | Use Case |
|---|---|---|
| F2 | 2 | Dev/Test (minimal) |
| F8 | 8 | Small teams |
| F16 | 16 | Medium workloads |
| F32 | 32 | Production |
| **F64** | **64** | **Free Trial (60 days)** |
| F128 | 128 | Enterprise |
| F256+ | 256+ | Large-scale production |

### Workspace Roles
| Role | Create/Delete Workspace | Add Admins | Add Members | Contribute Data | View Data |
|---|---|---|---|---|---|
| **Admin** | ✅ | ✅ | ✅ | ✅ | ✅ |
| **Member** | ❌ | ❌ | ✅ (same or lower) | ✅ | ✅ |
| **Contributor** | ❌ | ❌ | ❌ | ✅ | ✅ |
| **Viewer** | ❌ | ❌ | ❌ | ❌ | ✅ |

> ⚠️ **Important:** Roles apply to ALL objects in a workspace. You cannot restrict access per individual item.

---

## 3. Free Account Setup

### Step-by-Step

**Step 1: Create a Free Azure Account**
```
https://azure.microsoft.com/en-us/free/
→ Click "Try Azure for free"
→ Fill in personal details + credit card (no charges — verification only)
→ You get $200 USD credit for 30 days + 55+ always-free services
```

**Step 2: Create a Domain-Linked User via Microsoft Entra ID**
```
portal.azure.com
→ Search: "Microsoft Entra ID"
→ Manage → Users → + New User → Create New User
→ User principal name: fabrictutorial@<your-domain>.onmicrosoft.com
→ Set display name + password
→ Click "Review + Create"
```

**Step 3: Activate Microsoft Fabric Free Trial**
```
app.fabric.microsoft.com
→ Sign in with the new domain-linked account
→ Click your account icon (top right)
→ "Start Trial" → Activate 60-day F64 capacity
→ Confirm: License type = Trial, 59 days remaining
```

**Step 4: Create Your Workspace**
```
Left sidebar → Workspaces → + New Workspace
→ Name: YourName_Workspace
→ Click Apply
```

> 📌 **Trial Notes:**
> - 60-day free trial with F64 capacity (64 compute units)
> - After 60 days, all resources are **permanently deleted within 7 days** if not upgraded
> - You can create a new account after trial expires

---

## 4. OneLake — The Foundation

OneLake is the **"OneDrive for Data"** — a single, unified data lake automatically provisioned for every Fabric tenant.

### Key Properties
- Built on top of **Azure Data Lake Storage Gen2 (ADLS Gen2)**
- Supports **structured, semi-structured, and unstructured** data
- **Only one OneLake per tenant** — never zero, never two
- All tabular/structured data stored in **Delta Parquet format** automatically
- Eliminates data redundancy (LRS, GRS, GZRS management handled by Fabric)

### OneLake URL Structure
```
abfss://<WorkspaceName>@onelake.dfs.fabric.microsoft.com/<ObjectName>.<ObjectType>/Files/
```

**Example:**
```
abfss://AnlambaPro@onelake.dfs.fabric.microsoft.com/anlamba_lakehouse.Lakehouse/Files/raw-data/
```

| URL Component | Equivalent in ADLS Gen2 |
|---|---|
| `WorkspaceName` | Container name |
| `onelake` | Storage account name |
| `<ObjectName>.Lakehouse` | Folder within container |
| `/Files/` | Sub-folder (root directory) |

### OneLake vs Traditional ADLS Gen2
```
Traditional Azure (Multiple Storage Accounts):
  ├── storage-account-1/  (ADF data)
  ├── storage-account-2/  (Synapse data)
  ├── storage-account-3/  (ML data)
  └── storage-account-4/  (Finance division data)
      → 4 storage accounts, 6 integrations, 4× data copies

Microsoft Fabric (OneLake):
  └── onelake/
      └── WorkspaceName/
          ├── anlamba_lakehouse.Lakehouse/Files/
          ├── anlamba_warehouse.Warehouse/
          └── anlamba_lakehouse2.Lakehouse/Files/
              → 1 storage, 0 integrations, 1 copy of data
```

### OneLake File Explorer
Download the OneLake File Explorer (like Azure Storage Explorer) to browse and sync Fabric data directly from your PC:
```
In Fabric → OneLake → Download OneLake File Explorer
```

---

## 5. Delta Parquet File Format

### What Is It?
Delta Parquet = **Parquet file** + **Delta Log (Transaction Log)**

```
Without Delta Log:              With Delta Log (Delta Format):
  data1.parquet                   data1.parquet
  data2.parquet                   data2.parquet
                                  _delta_log/
                                    00000.json  ← transaction record
                                    00001.json  ← transaction record
```

### Parquet Format Basics
- **Columnar file format** — reads data column-by-column (not row-by-row)
- Optimized for big data analytics and distributed processing
- Supports compression (Snappy, GZIP)

### Delta Log (Transaction Log)
The Delta Log is a folder (`_delta_log/`) that records **every transaction** (file added, file removed, schema change) as a JSON entry.

### Why Delta Format Over Plain Parquet?

| Feature | Plain Parquet | Delta Parquet |
|---|---|---|
| ACID Transactions | ❌ | ✅ |
| Time Travel | ❌ | ✅ |
| Data Versioning | ❌ | ✅ |
| Schema Evolution | ❌ | ✅ |
| Commit / Rollback | ❌ | ✅ |
| Query Performance (Z-Order) | ❌ | ✅ |

### ACID Transactions
| Property | Meaning |
|---|---|
| **A**tomicity | All or nothing — partial writes don't corrupt data |
| **C**onsistency | Data always in a valid state |
| **I**solation | Concurrent reads/writes don't conflict |
| **D**urability | Committed data persists even after failures |

### Time Travel Example (PySpark)
```python
# Read a specific version of the table
df = spark.read.format("delta").option("versionAsOf", 3).load("abfss://...")

# Read data as of a specific timestamp
df = spark.read.format("delta").option("timestampAsOf", "2024-01-01").load("abfss://...")

# View full history
from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "abfss://...")
deltaTable.history().show()
```

---

## 6. Storage Solutions: Warehouse vs Lakehouse vs KQL Database

### Architecture Overview
```
                         ┌──────────────────────────────────┐
                         │           OneLake (ADLS Gen2)     │
                         └──────────┬───────────┬────────────┘
                                    │           │
              ┌─────────────────────┘           └────────────────────┐
              ▼                                                       ▼
   ┌──────────────────┐                              ┌───────────────────────┐
   │  Fabric Lakehouse │                              │  Fabric Data Warehouse│
   │  (Delta Parquet)  │                              │  (Lake-Centric DW)    │
   │  PySpark + SQL EP │                              │  T-SQL only           │
   └──────────────────┘                              └───────────────────────┘
              ▲
              │  Streaming data
   ┌──────────────────┐
   │   KQL Database   │
   │  (Real-Time)     │
   └──────────────────┘
```

### Comparison Table
| Feature | Fabric Data Warehouse | Fabric Lakehouse | KQL Database |
|---|---|---|---|
| **Best for** | Enterprise DW, T-SQL focus | Big data + PySpark | Streaming / real-time |
| **Storage** | Delta Parquet in OneLake | Delta Parquet in OneLake | Optimized columnar |
| **Query Languages** | T-SQL only | PySpark, SQL Endpoint | Kusto Query Language |
| **Data Model** | Dimensional (Star Schema) | Flexible (files + tables) | Time-series / events |
| **Code Required** | Low (T-SQL) | Medium (PySpark + SQL) | Low (KQL syntax) |
| **Azure Equivalent** | Synapse SQL Pool | Databricks Lakehouse | Azure Data Explorer |

### Lakehouse Folder Structure
```
anlamba_lakehouse.Lakehouse/
├── Files/                ← Raw files (CSV, JSON, Parquet, etc.)
│   ├── raw-data/
│   │   ├── customers/
│   │   │   └── customers.csv
│   │   └── products/
│   │       └── products.csv
│   └── scripts/
│       └── config.json
└── Tables/               ← Delta tables (queryable via SQL endpoint)
    ├── dim_customer
    ├── dim_product
    └── fact_sales
```

### When to Use Lakehouse vs Warehouse
| Use Lakehouse When... | Use Warehouse When... |
|---|---|
| Primary transformation tool is **PySpark** | Team is **T-SQL focused** |
| High volume of varied data sources | Need exact **ANSI SQL** compliance |
| Need lightweight DW with file flexibility | Building **enterprise-grade DW** |
| Working with ML/Data Science pipelines | Need **stored procedures** and SQL-native objects |

---

## 7. Data Ingestion Methods

### Overview
```
External Sources
  ├── GitHub / REST API
  ├── Azure Data Lake Gen2
  ├── Azure SQL Database
  ├── AWS S3
  ├── Snowflake
  └── On-Premise DB
            │
   ┌────────▼────────┐
   │  Ingestion Layer │
   │  ─────────────  │
   │  1. Copy Activity│
   │  2. Dataflow Gen2│
   │  3. PySpark NB   │
   │  4. Eventstream  │
   │  5. Shortcuts    │
   │  6. Mirroring    │
   └────────┬────────┘
            ▼
        OneLake
```

### Method 1: Copy Activity (Data Pipeline)
- GUI-based, no-code
- Best for bulk data movement
- Supports 100+ connectors
- Supports incremental + full load

### Method 2: Dataflow Gen2
- Power Query UI (no-code)
- Code-free data transformations
- Ideal for data analysts familiar with Power BI
- Supports 150+ data sources

### Method 3: PySpark Notebooks
- Code-first, full flexibility
- Best for complex transformations
- Supports Python, Scala, SQL, R
- Distributed processing via Apache Spark

### Method 4: Eventstream
- For streaming / IoT data
- Continuous real-time ingestion
- Connects to IoT Hub, Kafka, Event Hubs

### Method 5: Shortcuts (No Data Movement)
- Creates a **pointer** to an external data source
- No data is copied into OneLake
- Supported sources: ADLS Gen2, AWS S3, GCP, OneLake (cross-workspace)
- Data is read directly from source at query time

```
Shortcut analogy:
  Game files → Local Disk C:/Games/FIFA/
  Desktop shortcut → points to C:/Games/FIFA/fifa.exe
  Double-click shortcut → game runs FROM disk C (no copy made)

Fabric Shortcut:
  Data → AWS S3 bucket
  Shortcut in Lakehouse → points to S3 path
  Query shortcut → data served FROM S3 (no copy in OneLake)
```

### Method 6: Mirroring
Auto-replicates data from external sources into OneLake:

| Source | Mirroring Type | Near Real-Time? |
|---|---|---|
| Azure Cosmos DB | Database mirroring (data + schema) | ✅ |
| Azure SQL Database | Database mirroring (data + schema) | ✅ |
| Azure Databricks | Metadata mirroring (schema only) | ✅ |
| Snowflake | Database mirroring | ✅ |
| Azure SQL Managed Instance | Database mirroring | ✅ |

---

## 8. Fabric Data Factory — Deep Dive

### What Is It?
Fabric Data Factory is a **low/no-code ETL orchestration tool** embedded in Microsoft Fabric — the evolution of Azure Data Factory (ADF).

```
Azure Data Factory  →  Fabric Data Factory
      ADF                  "Data Pipeline"
```

### Activity Categories
| Category | Activities |
|---|---|
| Move & Transform | Copy Activity, Dataflow |
| Metadata | Lookup, Get Metadata, Delete |
| Control Flow | If Condition, Switch, For Each, Until, Filter, Wait, Set Variable |
| Orchestrate | Execute Pipeline (parent-child pipelines) |
| Transform | Notebook, Spark Job Definition, Script |

### Pipeline Canvas Components
```
┌─────────────────────────────────────────────┐
│              Pipeline Canvas                 │
│                                             │
│  [Copy Activity 1] ──success──► [Notebook] │
│         │                                   │
│       failure                               │
│         │                                   │
│         ▼                                   │
│  [Send Email Activity]                      │
│                                             │
└─────────────────────────────────────────────┘

Node Colors:
  🟢 Green  = On Success
  🔴 Red    = On Failure
  ⚫ Gray   = On Skip / Skipped
  ⚪ White  = On Completion (always runs)
```

### Copy Activity Configuration

**Source: HTTP (GitHub API)**
```
Connection Type: HTTP
Base URL: https://raw.githubusercontent.com/anshlambagit/Fabric-Tutorial/main/
Relative URL: Data/customers.csv
Authentication: Anonymous
File Format: DelimitedText (CSV)
```

**Destination: Fabric Lakehouse**
```
Connection: <your_lakehouse_name>
Root Folder: Files
Directory: raw-data/customers
File Name: customers.csv
File Format: DelimitedText
```

### Copy Activity — ADLS Gen2 Source
**Step 1: Assign RBAC Role in Azure Portal**
```
portal.azure.com
→ Storage Account → Access Control (IAM)
→ + Add → Add role assignment
→ Role: "Storage Blob Data Contributor"
→ Assign to: <your Fabric account email>
→ Review + Assign
(Wait 3–5 minutes for role to propagate)
```

**Step 2: Create Connection in Fabric**
```
Connection Type: Azure Data Lake Storage Gen2
URL: https://<storage-account-name>.dfs.core.windows.net/
Authentication: Organizational Account
Connection Name: ADLS_Connection
```

---

## 9. Real-Time Scenarios in Data Factory

### Parameterized / Dynamic Pipelines

**The Problem:** Static pipelines hardcode file paths — not scalable for multiple files.

**The Solution:** Use a JSON config file + Lookup Activity + For Each Activity.

### Step 1: Create the JSON Config File
```json
[
  {
    "pSrcRelativeURL": "Data/customers.csv",
    "pSyncFolder": "customers",
    "pSyncFile": "customers.csv"
  },
  {
    "pSrcRelativeURL": "Data/products.csv",
    "pSyncFolder": "products",
    "pSyncFile": "products.csv"
  },
  {
    "pSrcRelativeURL": "Data/calendar.csv",
    "pSyncFolder": "calendar",
    "pSyncFile": "calendar.csv"
  },
  {
    "pSrcRelativeURL": "Data/sales_2016.csv",
    "pSyncFolder": "sales_2016",
    "pSyncFile": "sales_2016.csv"
  }
]
```

Upload to: `Lakehouse/Files/scripts/config.json`

### Step 2: Lookup Activity
```
Settings:
  Connection: <your_lakehouse>
  Root Folder: Files
  File Path: scripts/config.json
  File Format: JSON
  ☑ First Row Only: UNCHECKED  ← Important!
```

Output key to reference: `@activity('LookupActivityName').output.value`

### Step 3: For Each Activity
```
Settings:
  Sequential: OFF (run all files in parallel)
  Items: @activity('file_names').output.value
```

### Step 4: Copy Activity Inside For Each
```python
# Source Relative URL (Dynamic Content):
@item().pSrcRelativeURL

# Destination Directory (Dynamic Content — combining text + variable):
@concat('raw-data/', item().pSyncFolder)

# Destination File Name (Dynamic Content):
@item().pSyncFile
```

### Complete Pipeline Architecture
```
┌─────────────────────────────────────────────────────────┐
│                    Master Pipeline                        │
│                                                         │
│  [Lookup: Read config.json]                             │
│           │ (on success)                                │
│           ▼                                             │
│  [For Each: iterate files array]                        │
│    ┌──────────────────────────────┐                     │
│    │  [Copy Activity]             │                     │
│    │  Source: GitHub API          │                     │
│    │  Dest: Lakehouse/Files/      │                     │
│    └──────────────────────────────┘                     │
└─────────────────────────────────────────────────────────┘
```

### Invoke Pipeline (Parent-Child Pattern)
```
Parent Pipeline → runs Child Pipeline 1 → then Child Pipeline 2
                              ↓
             Use "Execute Pipeline" activity
             Configure: Pipeline = Tutorial_1
             Wait on completion: YES
```

### Email Notification on Pipeline Failure
```
Activity: Office 365 Outlook (or Web Activity calling Logic App)
Trigger: Connect on Failure node from any activity
Message body (Dynamic Content):
  Pipeline @{pipeline().Pipeline} failed at @{utcNow()}
  Error: @{activity('CopyActivity').error.message}
```

---

## 10. Dataflow Gen2

### What Is It?
A **Power Query-based, no-code data transformation tool** inside Fabric. Functionally equivalent to Power BI's Power Query Editor.

### Dataflow Gen1 vs Gen2
| Feature | Gen1 | Gen2 |
|---|---|---|
| UI | Classic dataflow UI | Power Query Editor (same as Power BI) |
| Output destination | Power BI dataset only | Lakehouse, Warehouse, or other Fabric items |
| Staging | No | Yes (auto-staging in Lakehouse) |
| Performance | Standard | Optimized with staging |

### Common Transformations Available
- Filter rows / columns
- Merge queries (JOIN operations)
- Append queries (UNION)
- Group By (aggregations)
- Pivot / Unpivot
- Replace values
- Split columns
- Add custom columns (M / Power Query formula)
- Change data types

### ETL Pipeline using Dataflow Gen2
```
Source (CSV in Files/) 
    → Dataflow Gen2 transformations 
        → Output to Lakehouse Tables/ (as Delta table)
```

---

## 11. Fabric Data Engineering (PySpark)

### Notebook Interface
Fabric Notebooks run **Apache Spark** natively — no cluster setup required. The Fabric capacity provides Spark compute automatically.

### Supported Languages
| Language | Use Case |
|---|---|
| Python (PySpark) | Primary data engineering language |
| SQL | Quick queries, CTEs |
| Scala | Legacy Spark code |
| R | Statistical analysis |

### Data Ingestion using PySpark
```python
# Read CSV from Lakehouse Files/
df = spark.read.format("csv")     .option("header", "true")     .option("inferSchema", "true")     .load("Files/raw-data/customers/customers.csv")

df.show(5)
df.printSchema()
```

### Write to Delta Table (Lakehouse Tables/)
```python
# Write as managed Delta table
df.write     .format("delta")     .mode("overwrite")     .saveAsTable("bronze_customers")

# Append mode
df.write     .format("delta")     .mode("append")     .saveAsTable("bronze_customers")
```

### Spark Utilities (mssparkutils)
```python
# List files
mssparkutils.fs.ls("Files/raw-data/")

# Copy files
mssparkutils.fs.cp("Files/raw-data/customers.csv", "Files/archive/customers.csv")

# Move files
mssparkutils.fs.mv("Files/raw-data/customers.csv", "Files/processed/customers.csv")

# Delete files
mssparkutils.fs.rm("Files/raw-data/old_data.csv", True)

# Read notebook secret
mssparkutils.credentials.getSecret("KeyVaultName", "SecretName")
```

---

## 12. Fabric Shortcuts

### Creating a Shortcut
```
Lakehouse Explorer
→ Files/ → right-click → New Shortcut
→ Choose source:
    - Azure Data Lake Storage Gen2
    - Amazon S3
    - Google Cloud Storage
    - OneLake (cross-workspace)
→ Provide connection details
→ Name your shortcut
```

### ADLS Gen2 Shortcut Config
```
URL: https://<storage>.dfs.core.windows.net/
Authentication: Organizational Account (or SAS Token)
Shortcut name: external_adls_data
Sub path: /raw-container/sales-data/
```

### S3 Shortcut Config
```
URL: https://s3.amazonaws.com/<bucket-name>
Authentication: AWS IAM (Access Key + Secret)
Shortcut name: aws_s3_sales
Sub path: /data/sales/
```

---

## 13. Medallion Architecture in Lakehouse

### Architecture Layers
```
Raw Source Data
      │
      ▼
┌─────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Raw ingestion — no transformation)        │
│  Format: Delta Parquet                                   │
│  Notebook: 01_ingest_bronze.ipynb                        │
│  Tables: bronze_customers, bronze_sales, bronze_products │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│  SILVER LAYER (Cleaned, deduplicated, standardized)      │
│  Format: Delta Parquet                                   │
│  Notebook: 02_transform_silver.ipynb                     │
│  Tables: silver_customers, silver_sales, silver_products │
└───────────────────────────┬─────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│  GOLD LAYER (Business-ready, aggregated, modeled)        │
│  Format: Delta Parquet                                   │
│  Notebook: 03_build_gold.ipynb                           │
│  Tables: dim_customer, dim_product, fact_sales           │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
                    Power BI Reports
```

### Bronze Layer — Sample Notebook
```python
# 01_ingest_bronze.ipynb
from pyspark.sql.functions import current_timestamp, lit

# Read raw CSV
df_raw = spark.read.format("csv")     .option("header", "true")     .option("inferSchema", "true")     .load("Files/raw-data/customers/customers.csv")

# Add metadata columns
df_bronze = df_raw     .withColumn("ingested_at", current_timestamp())     .withColumn("source", lit("github_api"))

# Write to Bronze Delta table
df_bronze.write     .format("delta")     .mode("overwrite")     .option("overwriteSchema", "true")     .saveAsTable("bronze_customers")

print(f"Bronze layer: {df_bronze.count()} rows written")
```

### Silver Layer — Sample Notebook
```python
# 02_transform_silver.ipynb
from pyspark.sql.functions import col, upper, trim, when, isnan

# Read from Bronze
df_bronze = spark.table("bronze_customers")

# Transformations
df_silver = df_bronze     .dropDuplicates(["CustomerKey"])     .filter(col("CustomerKey").isNotNull())     .withColumn("FirstName", trim(upper(col("FirstName"))))     .withColumn("LastName", trim(upper(col("LastName"))))     .withColumn("EmailAddress", when(col("EmailAddress").isNull(), "unknown").otherwise(col("EmailAddress")))

# Write to Silver Delta table
df_silver.write     .format("delta")     .mode("overwrite")     .saveAsTable("silver_customers")
```

### Data Optimization in Fabric Lakehouse
```python
# OPTIMIZE — compacts small files into larger ones
spark.sql("OPTIMIZE silver_customers")

# Z-ORDER — colocate related data for faster queries
spark.sql("OPTIMIZE silver_customers ZORDER BY (CustomerKey)")

# VACUUM — remove old files beyond retention period (default 7 days)
spark.sql("VACUUM silver_customers RETAIN 168 HOURS")

# Table statistics
spark.sql("ANALYZE TABLE silver_customers COMPUTE STATISTICS")
```

---

## 14. Fabric Data Warehouse

### Key Differences from Traditional Data Warehouse
| Feature | Traditional DW (SQL Server, Redshift) | Fabric Data Warehouse |
|---|---|---|
| Storage | Internal database storage | OneLake (Delta Parquet) |
| Architecture | Row-based storage | Lake-centric (columnar) |
| Language | T-SQL | T-SQL |
| Scaling | Manual | Automatic |
| Cost model | Pay per server/node | Pay per CU (capacity unit) |

### CTAS (Create Table As Select)
```sql
-- Create aggregated table from another table
CREATE TABLE gold_sales_summary AS
SELECT
    d.CalendarYear,
    d.MonthName,
    c.City,
    c.StateProvinceName,
    SUM(f.SalesAmount) AS TotalSales,
    COUNT(f.SalesOrderNumber) AS OrderCount,
    AVG(f.SalesAmount) AS AvgOrderValue
FROM fact_sales f
JOIN dim_date d ON f.OrderDateKey = d.DateKey
JOIN dim_customer c ON f.CustomerKey = c.CustomerKey
GROUP BY d.CalendarYear, d.MonthName, c.City, c.StateProvinceName;
```

### Views in Fabric Warehouse
```sql
-- Create a view for security/abstraction
CREATE VIEW vw_CustomerSalesSummary AS
SELECT
    c.CustomerKey,
    c.FirstName + ' ' + c.LastName AS CustomerName,
    c.EmailAddress,
    SUM(f.SalesAmount) AS TotalSpend,
    COUNT(f.SalesOrderNumber) AS TotalOrders
FROM dim_customer c
LEFT JOIN fact_sales f ON c.CustomerKey = f.CustomerKey
GROUP BY c.CustomerKey, c.FirstName, c.LastName, c.EmailAddress;
```

### Visual Query Editor
Fabric Warehouse includes a drag-and-drop **Visual Query Editor** (no SQL needed):
```
Warehouse Explorer → New Visual Query
→ Drag tables onto canvas
→ Select JOIN type (Inner, Left, Right)
→ Select columns to include
→ Add filters, aggregations
→ Results preview at bottom
```

---

## 15. Dimensional Data Modeling & SCD

### Star Schema Design
```
                    ┌─────────────────┐
                    │   fact_sales    │
                    │─────────────────│
                    │ SalesKey (PK)   │
                    │ CustomerKey(FK) │──────► dim_customer
                    │ ProductKey (FK) │──────► dim_product
                    │ DateKey (FK)    │──────► dim_date
                    │ TerritoryKey(FK)│──────► dim_territory
                    │ SalesAmount     │
                    │ Quantity        │
                    │ UnitPrice       │
                    └─────────────────┘
```

### SCD Type 1 — Overwrite (No History)
```sql
-- Simply UPDATE the record — old value is lost
MERGE INTO dim_customer AS target
USING staging_customers AS source
ON target.CustomerKey = source.CustomerKey
WHEN MATCHED THEN
    UPDATE SET
        target.EmailAddress = source.EmailAddress,
        target.Phone = source.Phone
WHEN NOT MATCHED THEN
    INSERT (CustomerKey, FirstName, LastName, EmailAddress)
    VALUES (source.CustomerKey, source.FirstName, source.LastName, source.EmailAddress);
```

### SCD Type 2 — Keep Full History
```sql
-- Add new row with version tracking
-- Step 1: Expire old record
UPDATE dim_customer
SET IsCurrent = 0,
    EndDate = GETDATE()
WHERE CustomerKey = @CustomerKey AND IsCurrent = 1;

-- Step 2: Insert new version
INSERT INTO dim_customer (CustomerKey, FirstName, LastName, EmailAddress, StartDate, EndDate, IsCurrent)
VALUES (@CustomerKey, @FirstName, @LastName, @NewEmail, GETDATE(), NULL, 1);
```

### SCD Type 2 Table Structure
```sql
CREATE TABLE dim_customer (
    SurrogateKey    INT IDENTITY(1,1) PRIMARY KEY,
    CustomerKey     INT,
    FirstName       VARCHAR(100),
    LastName        VARCHAR(100),
    EmailAddress    VARCHAR(200),
    StartDate       DATE,
    EndDate         DATE,
    IsCurrent       BIT DEFAULT 1
);
```

---

## 16. Fabric Data Science

### What's Available?
- Jupyter-style notebooks with ML libraries pre-installed
- **MLflow** integration for experiment tracking
- **Data Wrangler** — no-code EDA and transformation tool
- Model registry and versioning

### Sample ML Workflow
```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# Load data from Lakehouse
df = spark.table("silver_customers").toPandas()
X = df[["Age", "YearlyIncome", "TotalChildren"]]
y = df["BikeBuyer"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train with MLflow tracking
mlflow.set_experiment("customer_churn_prediction")

with mlflow.start_run():
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)

    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_param("n_estimators", 100)
    mlflow.sklearn.log_model(model, "random_forest_model")

    print(f"Model Accuracy: {accuracy:.4f}")
```

### Data Wrangler
```
In Notebook → Toolbar → Data Wrangler
→ Select a DataFrame
→ Visual UI opens with:
    - Column summary stats
    - Missing value analysis
    - Drop columns, filter rows
    - Fill missing values
    - One-hot encoding
→ Generated code is automatically inserted into your notebook
```

---

## 17. Data Governance & Lineage

### Endorsements
Label your Fabric items to signal data quality:
```
Workspace Item → Settings → Endorsement
  ├── Promoted    → "This item is ready for general use"
  └── Certified   → "This item meets organizational standards" (requires admin)
```

### Lineage View
```
Workspace → View → Lineage

Visual map showing:
  Data Pipeline → Lakehouse → Semantic Model → Power BI Report

You can trace:
  - Where data comes from (data source)
  - How data flows through transformations
  - What reports depend on which tables
```

### Semantic Models & Direct Lake
- **Semantic Model** (formerly Power BI Dataset) — defines relationships, measures, and hierarchies on top of Lakehouse or Warehouse tables
- Used by Power BI reports as their data source

---

## 18. CI/CD with Deployment Pipelines

### Deployment Pipeline Stages
```
┌─────────────┐     promote     ┌─────────────┐     promote     ┌─────────────┐
│  DEVELOPMENT │ ─────────────► │     UAT     │ ─────────────► │ PRODUCTION  │
│  Workspace   │                │  Workspace  │                │  Workspace  │
└─────────────┘                 └─────────────┘                └─────────────┘
```

### Setup
```
Left sidebar → Deployment Pipelines → + New Pipeline
→ Name: Fabric_CICD_Pipeline
→ Assign Workspace to Development stage
→ Assign Workspace to UAT stage
→ Assign Workspace to Production stage
→ Deploy: Development → UAT (with comparison view)
→ Deploy: UAT → Production (with approval)
```

### What Gets Deployed
- Data Pipelines
- Notebooks
- Lakehouses (structure only, not data)
- Warehouses (structure only)
- Dataflows
- Semantic Models
- Power BI Reports

---

## 19. Direct Lake vs Direct Query vs Import Mode

| Mode | Data Location | Refresh Needed? | Performance | Best For |
|---|---|---|---|---|
| **Import Mode** | Copied into Power BI memory | Yes (scheduled) | Fastest | Small-medium datasets, offline use |
| **Direct Query** | Queried live from source | No | Slower (live query) | Large datasets, real-time accuracy needed |
| **Direct Lake** | Reads Delta files directly from OneLake | No | Fastest (no import overhead) | Fabric Lakehouse/Warehouse, large-scale |

### Direct Lake — How It Works
```
Power BI Report
      │ query
      ▼
Semantic Model (Direct Lake mode)
      │ reads Delta Parquet metadata
      ▼
OneLake (Lakehouse Tables/)
      │ no data movement, no copy
      ▼
Returns results directly from Delta files
```

> ✅ **Direct Lake is the recommended mode** when using Power BI with Fabric Lakehouse or Warehouse. It combines the speed of Import mode with the freshness of Direct Query.

---

## 20. Projects & Next Steps

### Hands-On Project Ideas

#### Project 1: End-to-End Sales Analytics Platform
```
Architecture:
  GitHub API (Adventure Works CSV files)
      → Fabric Data Pipeline (dynamic, parameterized)
          → Lakehouse Bronze Layer (raw Delta tables)
              → PySpark Notebook (Silver transformations)
                  → Fabric Warehouse (Gold: star schema)
                      → Power BI Report (Direct Lake)

Skills practiced:
  ✅ Data Factory dynamic pipelines
  ✅ Medallion Architecture
  ✅ PySpark transformations
  ✅ Dimensional data modeling
  ✅ Power BI Direct Lake
```

#### Project 2: Real-Time IoT Dashboard
```
Architecture:
  Simulated IoT events (Event Hub)
      → Eventstream (real-time ingestion)
          → KQL Database (streaming storage)
              → Power BI Real-Time Dashboard

Skills practiced:
  ✅ Eventstream setup
  ✅ KQL Database
  ✅ KQL query language
  ✅ Real-time Power BI
```

#### Project 3: ML Customer Segmentation
```
Architecture:
  Cleaned Silver Layer (customer data)
      → Fabric Data Science Notebook
          → K-Means clustering model (scikit-learn / MLflow)
              → Write segments back to Gold Lakehouse table
                  → Power BI Segment Analysis Report

Skills practiced:
  ✅ Fabric Data Science
  ✅ MLflow experiment tracking
  ✅ Data Wrangler
  ✅ Model registry
```

#### Project 4: Multi-Source ETL with CI/CD
```
Architecture:
  Source 1: GitHub API → Data Pipeline 1
  Source 2: ADLS Gen2 → Data Pipeline 2
  Source 3: Azure SQL DB (Mirroring) → auto-sync
      → All land in Lakehouse Bronze
          → Shared transformation notebook
              → Gold tables
                  → Deployment Pipeline (Dev → UAT → Prod)

Skills practiced:
  ✅ Multi-source ingestion
  ✅ Mirroring setup
  ✅ CI/CD deployment pipelines
  ✅ Parent-child pipeline orchestration
```

---

## Quick Reference Cheat Sheet

### Key URLs
| Resource | URL |
|---|---|
| Fabric Portal | `app.fabric.microsoft.com` |
| Azure Portal | `portal.azure.com` |
| OneLake Explorer Download | Available in Fabric OneLake section |
| Course Dataset (GitHub) | `github.com/anshlambagit/Fabric-Tutorial` |

### Dynamic Content Expressions
| Expression | Meaning |
|---|---|
| `@pipeline().Pipeline` | Current pipeline name |
| `@utcNow()` | Current UTC timestamp |
| `@activity('Name').output.value` | Output array from Lookup activity |
| `@item().columnName` | Current item in For Each loop |
| `@concat('prefix/', item().folder)` | String concatenation with variable |
| `@pipeline().parameters.myParam` | Pipeline parameter value |

### PySpark Quick Reference
```python
# Read Delta table
df = spark.table("table_name")

# Read from Files/
df = spark.read.format("csv").option("header","true").load("Files/path/file.csv")

# Write Delta table
df.write.format("delta").mode("overwrite").saveAsTable("table_name")

# Delta time travel
df = spark.read.format("delta").option("versionAsOf", 2).load("Files/path/")

# Optimize table
spark.sql("OPTIMIZE table_name ZORDER BY (key_column)")

# Show Delta history
spark.sql("DESCRIBE HISTORY table_name").show()
```

### Fabric Data Factory — Activity Cheat Sheet
```
Copy Activity     → Move data between source and destination
Lookup Activity   → Read file/table and return output for downstream use
For Each Activity → Iterate through array and run activities per item
If Condition      → Conditional branching (true/false path)
Execute Pipeline  → Call child pipeline from parent pipeline
Set Variable      → Store and update variables within pipeline
Wait Activity     → Pause pipeline execution for X seconds
Delete Activity   → Delete files from storage
```

---

## Learning Path

```
1. Microsoft Fabric Foundations (this course)
        ↓
2. Fabric Certification: DP-600 (Fabric Analytics Engineer)
        ↓
3. Advanced PySpark for Fabric Data Engineering
        ↓
4. Fabric Data Science + MLflow
        ↓
5. Power BI + Fabric Direct Lake (PL-300 / DP-700)
        ↓
6. Microsoft Fabric + Azure DevOps CI/CD
```

---

