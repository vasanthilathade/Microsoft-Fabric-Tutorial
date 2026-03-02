# Power Query / Dataflow Gen2 vs. Fabric Data Pipeline
> A detailed tutorial and compare-and-contrast guide for Microsoft Fabric Data Engineers

---

## Table of Contents
1. [Overview](#overview)
2. [What is Dataflow Gen2 (Power Query)?](#what-is-dataflow-gen2-power-query)
3. [What is a Fabric Data Pipeline?](#what-is-a-fabric-data-pipeline)
4. [Core Differences at a Glance](#core-differences-at-a-glance)
5. [Power Query / Dataflow Gen2 Deep Dive](#power-query--dataflow-gen2-deep-dive)
   - [The Power Query Editor UI](#the-power-query-editor-ui)
   - [No-Code Transformations](#no-code-transformations)
   - [Low-Code: Parameters & Dynamic Paths](#low-code-parameters--dynamic-paths)
   - [Pro-Code: Writing M Language](#pro-code-writing-m-language)
   - [Output Destinations](#output-destinations)
   - [Dataflow Gen2 vs Gen1](#dataflow-gen2-vs-gen1)
6. [Fabric Data Pipeline Deep Dive](#fabric-data-pipeline-deep-dive)
   - [Pipeline Canvas & Activities](#pipeline-canvas--activities)
   - [Copy Activity](#copy-activity)
   - [Dataflow Activity (calling Gen2 from Pipeline)](#dataflow-activity-calling-gen2-from-pipeline)
   - [Control Flow Activities](#control-flow-activities)
   - [Triggers & Scheduling](#triggers--scheduling)
7. [Power Query M Language Reference](#power-query-m-language-reference)
8. [End-to-End Tutorial: Dataflow Gen2 + Pipeline Together](#end-to-end-tutorial-dataflow-gen2--pipeline-together)
9. [Copilot AI Integration](#copilot-ai-integration)
10. [Monitoring & Debugging](#monitoring--debugging)
11. [Decision Framework](#decision-framework)
12. [Best Practices](#best-practices)

---

## Overview

Microsoft Fabric provides two primary tools for data movement and transformation in Data Factory:

| | **Dataflow Gen2** | **Fabric Data Pipeline** |
|---|---|---|
| **Heritage** | Power Query (Excel, Power BI, Power Platform) | Azure Data Factory (ADF) |
| **Primary Role** | Data transformation + light ingestion | Orchestration + data movement at scale |
| **Interface** | Power Query Online (visual canvas) | Drag-and-drop pipeline canvas |
| **Code Language** | Power Query M | JSON-based pipeline definition |
| **Transformation Depth** | 300+ built-in transformation functions | Low (type cast, column mapping) |
| **Orchestration Depth** | Low (single-flow execution) | High (ForEach, If/Else, Switch, dependencies) |
| **Best For** | Business analysts, data modelers, ETL logic | Data engineers, complex workflows, high-volume ingestion |

> 💡 **Mental Model**: Think of **Dataflow Gen2** as your _transformation layer_ (the "T" in ETL), and **Pipelines** as your _orchestration layer_ (the workflow engine that sequences everything together).

---

## What is Dataflow Gen2 (Power Query)?

Dataflow Gen2 is Microsoft Fabric's low-code/no-code data transformation service built on **Power Query Online** — the same engine used in Excel, Power BI Desktop, Power Apps, and Dynamics 365 [web:22].

### Key Characteristics
- **Visual transformation editor**: Point-and-click transformations with 300+ options.
- **Power Query M language** underneath every step — always inspectable and editable.
- **Multiple output destinations**: Write to Lakehouse, Warehouse, Azure SQL DB, Azure Data Explorer, and more.
- **High-performance compute**: Uses Fabric SQL Compute engine (backed by Lakehouse + Warehouse staging).
- **Copilot-enabled**: Natural language to generate M code and transformations.
- **AutoSave + Background publishing**: No manual publish button click required.
- **Monitoring Hub integration**: Refresh history and status visible across all dataflows.

### When to Use Dataflow Gen2
✅ You need to clean, reshape, or enrich data (business logic)  
✅ Your transformation logic maps clearly (filter, group, merge, pivot)  
✅ You prefer a **visual lineage** of transformation steps  
✅ You want **reusable** transformation components shared across teams  
✅ You work with **Power BI datasets or Semantic Models**  
✅ You want **no Spark expertise required**  

---

## What is a Fabric Data Pipeline?

Fabric Data Pipelines are the **orchestration engine** inside Microsoft Fabric's Data Factory, evolved from Azure Data Factory (ADF). They provide a canvas-based workflow builder to sequence activities, move data at scale, and control conditional logic [web:26].

### Key Characteristics
- **Activities**: 20+ activity types — Copy Data, Dataflow, Notebook, Script, Stored Procedure, Lookup, ForEach, If Condition, and more.
- **50+ source connectors + 40+ sink connectors** via Copy Activity.
- **Control flow**: Full conditional logic, looping, parallelism.
- **Triggers**: Schedule, Storage Event, Custom Event, Microsoft Fabric Activator (Reflex).
- **Petabyte-scale data movement**: Copy activity handles high-volume ingestion without transformation overhead.
- **Cross-platform invocation**: Invoke Fabric Notebooks, Spark jobs, Semantic Model refreshes, external APIs.
- **No publish step**: Save and Run immediately.

### When to Use Fabric Data Pipeline
✅ You need **orchestration** — sequencing multiple activities with dependencies  
✅ You're doing **large-scale data movement** (hundreds of GBs to PBs)  
✅ You need **conditional logic** (if error → retry, if file exists → skip)  
✅ You need to call **Notebooks, Spark jobs, SQL scripts, or external APIs**  
✅ You want to chain together a **Dataflow Gen2 → Copy → Notebook** workflow  
✅ You need **event-driven triggers** (file arrives in OneLake → fire pipeline)  

---

## Core Differences at a Glance

| Dimension | Dataflow Gen2 | Fabric Pipeline |
|---|---|---|
| **Primary Purpose** | Transform data | Orchestrate + move data |
| **Interface** | Power Query editor | Visual pipeline canvas |
| **Transformation Power** | ⭐⭐⭐⭐⭐ (300+ functions) | ⭐⭐ (lightweight only) |
| **Orchestration Power** | ⭐⭐ (single flow) | ⭐⭐⭐⭐⭐ (full control flow) |
| **Code Paradigm** | M language (functional) | JSON / low-code |
| **Connectors** | 150+ sources | 50+ sources / 40+ sinks |
| **Output Destinations** | 9 destinations | 40+ sinks |
| **Error Handling** | Basic (retry policy) | Advanced (On Failure paths) |
| **Looping** | ❌ | ✅ ForEach, Until |
| **Parallelism** | ❌ | ✅ ForEach parallel |
| **Event Triggers** | ❌ | ✅ Storage Event, Activator |
| **Invoke Notebook** | ❌ | ✅ |
| **Copilot Support** | ✅ | ✅ (coming) |
| **Compute Management** | Fabric-managed | Fabric-managed |
| **Lineage Transparency** | ✅ Step-by-step visual | Limited |
| **Audience** | Engineers + Analysts | Data Engineers |

---

## Power Query / Dataflow Gen2 Deep Dive

### The Power Query Editor UI

When you create a **Dataflow Gen2** in Fabric, you land in the **Power Query Online** editor:

```
┌─────────────────────────────────────────────────────────────┐
│  Ribbon: Home | Transform | Add Column | View | Tools       │
├────────────────┬────────────────────────────┬───────────────┤
│  Queries Pane  │   Data Preview Grid        │ Query Settings│
│  (left panel)  │   (center - live preview)  │  (right panel)│
│                │                            │ Applied Steps │
│  Query1        │  Col1 | Col2 | Col3 | ...  │  - Source     │
│  Query2        │  val  | val  | val  | ...  │  - Navigation │
│                │                            │  - FilterRows │
├────────────────┴────────────────────────────┴───────────────┤
│  Diagram View Toggle | Formula Bar (M expression)           │
└─────────────────────────────────────────────────────────────┘
```

- **Queries Pane** (left): All queries/tables in your dataflow. Each can have its own source and transformation chain.
- **Applied Steps** (right): Every transformation is recorded as a step — full lineage.
- **Formula Bar**: Shows the M language expression for each step — always editable.
- **Diagram View**: Visual representation of query dependencies and merge relationships.

---

### No-Code Transformations

These are point-and-click transformations available from the **Ribbon**:

| Category | Transformations |
|---|---|
| **Filter / Sort** | Filter rows, Sort ascending/descending, Remove duplicates, Remove errors |
| **Column Ops** | Remove columns, Rename, Reorder, Split column, Merge columns |
| **Type Conversion** | Change data type (Text, Int, Float, Date, Boolean, etc.) |
| **Text Transformations** | Trim, Clean, Uppercase, Lowercase, Extract (prefix/suffix/between) |
| **Number Transformations** | Round, Absolute, Percentage, Log, Power |
| **Date/Time** | Extract Year/Month/Day, Age, Earliest, Latest |
| **Aggregation** | Group By, Pivot, Unpivot |
| **Merges/Joins** | Merge queries (Left, Right, Inner, Full Outer, Left Anti, Right Anti) |
| **Append** | Append queries (Union of two tables) |
| **Conditional Column** | IF/THEN/ELSE to create new column |
| **Custom Column** | Write any M expression to create calculated column |

**Example: Group By (No-Code)**
1. Select column `Region`
2. Ribbon → **Transform** → **Group By**
3. Group by: `Region`
4. New column name: `TotalSales`, Operation: `Sum`, Column: `SalesAmount`
5. Click **OK** → Step added automatically

---

### Low-Code: Parameters & Dynamic Paths

Parameters make dataflows reusable and configurable:

```
// Step 1: Create Parameter
// Home → Manage Parameters → New Parameter
Name: SourceSchema
Type: Text
Default value: "dbo"

// Step 2: Use parameter in a query step
Source = Sql.Database("myserver.database.windows.net", "SalesDB"),
SchemaTable = Source{[Schema = SourceSchema, Item = "Orders"]}[Data]
```

**Parameterizing file paths (OneLake):**
```m
let
    // Parameter: FolderPath = "Files/raw/2024"
    Source = AzureStorage.Blobs("https://onelake.dfs.fabric.microsoft.com/workspace/lakehouse.Lakehouse/"),
    NavToFolder = Source{[Name = FolderPath]}[Content],
    ImportedCSV = Csv.Document(NavToFolder, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),
    PromotedHeaders = Table.PromoteHeaders(ImportedCSV, [PromoteAllScalars=true])
in
    PromotedHeaders
```

---

### Pro-Code: Writing M Language

Power Query M is a **functional, case-sensitive** language. Every dataflow is M under the hood.

**M Language Basics:**
```m
// Structure: let ... in
let
    // Each step is an assignment
    Source = Sql.Database("server", "database"),         // Step 1: Connect
    SalesTable = Source{[Schema="dbo",Item="Sales"]}[Data], // Step 2: Navigate
    FilteredRows = Table.SelectRows(SalesTable,            // Step 3: Filter
        each [OrderDate] >= #date(2024, 1, 1)),
    RenamedCols = Table.RenameColumns(FilteredRows,        // Step 4: Rename
        {{"CustID", "CustomerID"}, {"Amt", "Amount"}}),
    AddedYear = Table.AddColumn(RenamedCols, "Year",       // Step 5: Add column
        each Date.Year([OrderDate]), Int64.Type),
    GroupedData = Table.Group(AddedYear,                   // Step 6: Aggregate
        {"Year", "Region"},
        {{"TotalAmount", each List.Sum([Amount]), type number},
         {"OrderCount", each Table.RowCount(_), Int64.Type}})
in
    GroupedData
```

**Common M Functions Reference:**

| Function | Purpose | Example |
|---|---|---|
| `Table.SelectRows` | Filter rows | `Table.SelectRows(t, each [Amount] > 100)` |
| `Table.SelectColumns` | Keep specific columns | `Table.SelectColumns(t, {"Col1","Col2"})` |
| `Table.RemoveColumns` | Drop columns | `Table.RemoveColumns(t, {"Col3"})` |
| `Table.RenameColumns` | Rename columns | `Table.RenameColumns(t, {{"OldName","NewName"}})` |
| `Table.AddColumn` | Add calculated column | `Table.AddColumn(t, "NewCol", each [A] + [B])` |
| `Table.TransformColumnTypes` | Change data types | `Table.TransformColumnTypes(t, {{"Date", type date}})` |
| `Table.Group` | Group + aggregate | `Table.Group(t, {"Key"}, {{"Sum", each List.Sum([Val])}})` |
| `Table.Join` | Join two tables | `Table.Join(t1, "ID", t2, "ID", JoinKind.Inner)` |
| `Table.NestedJoin` | Merge (expand later) | `Table.NestedJoin(t1, {"ID"}, t2, {"ID"}, "Merged", JoinKind.LeftOuter)` |
| `Table.UnpivotOtherColumns` | Unpivot | `Table.UnpivotOtherColumns(t, {"ID"}, "Attribute", "Value")` |
| `Table.Pivot` | Pivot | `Table.Pivot(t, List.Distinct(t[Category]), "Category", "Value", List.Sum)` |
| `Table.ExpandTableColumn` | Expand nested | `Table.ExpandTableColumn(t, "Merged", {"Name","City"})` |
| `Text.Trim` | Trim whitespace | `Text.Trim([TextColumn])` |
| `Date.Year` | Extract year | `Date.Year([DateColumn])` |
| `List.Sum` | Sum a list | `List.Sum([NumberColumn])` |
| `Number.Round` | Round a number | `Number.Round([Amount], 2)` |
| `if...then...else` | Conditional | `if [Status] = "Active" then 1 else 0` |

---

### Output Destinations

Dataflow Gen2 supports writing to multiple destinations in a **single dataflow** [web:22]:

| Destination | Notes |
|---|---|
| **Fabric Lakehouse Tables** | Delta tables — most common for analytics |
| **Fabric Lakehouse Files** | Raw file storage (preview) |
| **Fabric Warehouse** | Structured SQL warehouse tables |
| **Fabric SQL Database** | Managed SQL database |
| **Fabric KQL Database** | For time-series / log analytics |
| **Azure SQL Database** | External Azure SQL target |
| **Azure Data Explorer (Kusto)** | External Kusto cluster |
| **Azure Data Lake Gen2** | External ADLS Gen2 (preview) |
| **SharePoint Files** | Write back to SharePoint |

**How to set output destination:**
1. In Power Query editor → right-click a query → **Add data destination**
2. Select destination type (e.g., Lakehouse Tables)
3. Select workspace → Lakehouse → table name
4. Choose: **Replace** (overwrite) or **Append** (incremental)
5. Map columns → **Save settings**
6. Click **Publish** (or **Save & Run**)

---

### Dataflow Gen2 vs Gen1

| Feature | Dataflow Gen2 | Dataflow Gen1 (Power BI) |
|---|---|---|
| Multiple output destinations | ✅ | ❌ (internal storage only) |
| Works with Fabric Pipelines | ✅ | ❌ |
| High-performance SQL compute | ✅ | ❌ |
| Copilot AI integration | ✅ | ❌ |
| AutoSave + background publish | ✅ | ❌ |
| Monitoring Hub integration | ✅ | ❌ |
| Direct Query via connector | ❌ | ✅ |
| Refresh changed data only | ✅ | ✅ |
| Power Query familiar interface | ✅ | ✅ |

> **Recommendation**: Always use **Dataflow Gen2** for new Fabric projects. Gen1 is maintained for backward compatibility with Power BI but is no longer the recommended path [web:22].

---

## Fabric Data Pipeline Deep Dive

### Pipeline Canvas & Activities

The **pipeline canvas** is a drag-and-drop workflow designer:

```
┌──────────────────────────────────────────────────────────────┐
│  Activities Panel (left)   │   Canvas (center)              │
│                            │                                │
│  ├─ Move & Transform       │  [Dataflow] ──✅──> [Copy]     │
│  │   ├─ Copy Data          │                    │          │
│  │   ├─ Dataflow           │                    ✅         │
│  │   └─ Copy Job           │                    ▼          │
│  ├─ Transform              │              [Notebook]        │
│  │   ├─ Notebook           │                               │
│  │   ├─ Script             │                               │
│  │   └─ Stored Procedure   │                               │
│  └─ Control Flow           │                               │
│      ├─ ForEach            │                               │
│      ├─ If Condition       │                               │
│      ├─ Switch             │                               │
│      ├─ Until              │                               │
│      ├─ Wait               │                               │
│      └─ Fail               │                               │
└──────────────────────────────────────────────────────────────┘
```

**Activity connection types:**
- **On Success** (green ✅): Next activity runs only if current succeeds
- **On Failure** (red ❌): Next activity runs only if current fails (use for error handling)
- **On Completion** (blue 🔵): Next activity runs regardless of success/failure
- **On Skip**: Runs if current activity was conditionally skipped

---

### Copy Activity

The **Copy Activity** is the workhorse for data movement:

```json
// Copy Activity configuration (conceptual JSON)
{
  "name": "CopyFromSQLToLakehouse",
  "type": "Copy",
  "inputs": [{
    "type": "SqlSource",
    "sqlReaderQuery": "SELECT * FROM dbo.Orders WHERE OrderDate >= '@{pipeline().parameters.StartDate}'",
    "queryTimeout": "02:00:00"
  }],
  "outputs": [{
    "type": "ParquetSink",
    "storeSettings": {
      "type": "LakehouseWriteSettings"
    },
    "formatSettings": {
      "type": "ParquetWriteSettings"
    }
  }],
  "translator": {
    "type": "TabularTranslator",
    "mappings": [
      {"source": {"name": "OrderID"}, "sink": {"name": "order_id"}},
      {"source": {"name": "Amount"}, "sink": {"name": "sale_amount"}}
    ]
  }
}
```

**Copy Activity key settings:**
- **Source**: 50+ connectors (SQL Server, Snowflake, REST, S3, SharePoint, Salesforce, etc.)
- **Sink**: 40+ destinations (Lakehouse, Warehouse, Azure SQL, ADLS, Blob, etc.)
- **Column mapping**: Visual or JSON-based
- **Fault tolerance**: Skip incompatible rows, log errors to storage
- **Parallelism**: Degree of Copy Parallelism (DIU equivalent in Fabric = CU consumption)
- **Staged copy**: Use Lakehouse as staging for performance optimization

---

### Dataflow Activity (calling Gen2 from Pipeline)

You can invoke a **Dataflow Gen2 from inside a Pipeline**:

```
Pipeline:
  Step 1: [Copy Activity]  ──✅──> Step 2: [Dataflow Activity] ──✅──> Step 3: [Notebook]
          (land raw data)           (transform + enrich)                (ML scoring)
```

**Configuration:**
1. Add **Dataflow** activity to canvas
2. Settings tab → **Workspace** → select your Fabric workspace
3. **Dataflow** → select your published Dataflow Gen2
4. Connect activity to upstream steps with success/failure arrows

> This is the most common pattern: **Pipelines orchestrate; Dataflows transform.**

---

### Control Flow Activities

**ForEach** — Iterate over a list of items:
```json
{
  "name": "ForEachTable",
  "type": "ForEach",
  "typeProperties": {
    "isSequential": false,          // Run in parallel
    "batchCount": 5,                // Max 5 parallel iterations
    "items": "@pipeline().parameters.TableList",
    "activities": [
      {
        "name": "CopyTable",
        "type": "Copy",
        "source": {
          "sqlReaderQuery": "@concat('SELECT * FROM ', item())"
        }
      }
    ]
  }
}
```

**If Condition** — Conditional branching:
```json
{
  "name": "CheckFileExists",
  "type": "IfCondition",
  "typeProperties": {
    "expression": "@equals(activity('GetMetadata').output.exists, true)",
    "ifTrueActivities": [{ "name": "CopyFile", "type": "Copy" }],
    "ifFalseActivities": [{ "name": "LogMissing", "type": "Script" }]
  }
}
```

**Pipeline Parameters** — Make pipelines dynamic:
```
// Define parameter at pipeline level:
Parameter Name: RunDate
Type: String
Default: @utcNow('yyyy-MM-dd')

// Reference in activity:
@pipeline().parameters.RunDate
@pipeline().runId
@utcNow()
@formatDateTime(pipeline().parameters.RunDate, 'yyyyMMdd')
```

---

### Triggers & Scheduling

| Trigger Type | Use Case | Example |
|---|---|---|
| **Manual** | Ad-hoc / testing | Click "Run" in Studio |
| **Schedule** | Regular interval | Daily at 2:00 AM UTC |
| **Storage Event** | React to file arrival | File lands in OneLake → trigger pipeline |
| **Custom Event** | Event Grid events | External system publishes event |
| **Fabric Activator (Reflex)** | Data-driven alerts | Metric crosses threshold → trigger pipeline |

**Adding a Schedule Trigger:**
1. Pipeline canvas → **Home** tab → **Schedule**
2. Set: Frequency (Minute/Hour/Day/Week/Month)
3. Set: Start DateTime, End DateTime, Timezone
4. Set: Recurrence (e.g., every Monday at 6:00 AM)
5. **Apply** → pipeline is now scheduled

---

## Power Query M Language Reference

### Data Type Literals
```m
// Primitive types
"text value"                    // type text
42                              // type number
42.5                            // type number (decimal)
true                            // type logical
#date(2024, 1, 15)              // type date
#datetime(2024, 1, 15, 10, 30, 0)  // type datetime
#duration(1, 2, 30, 0)         // type duration (1 day, 2 hrs, 30 min)
null                            // type null
```

### Working with Tables
```m
// Create table from scratch
MyTable = #table(
    type table [Name = text, Age = number, City = text],
    {
        {"Alice", 30, "Austin"},
        {"Bob", 25, "Dallas"},
        {"Carol", 35, "Houston"}
    }
),

// Filter: multiple conditions
Filtered = Table.SelectRows(MyTable,
    each [Age] >= 28 and [City] = "Austin"),

// Add conditional column
WithFlag = Table.AddColumn(Filtered, "SeniorFlag",
    each if [Age] >= 30 then "Senior" else "Junior", type text),

// Date-based filter using today
RecentOrders = Table.SelectRows(OrdersTable,
    each [OrderDate] >= Date.AddDays(DateTime.Date(DateTime.LocalNow()), -30))
```

### Advanced: Custom Functions
```m
// Define a reusable function
CleanPhoneNumber = (rawPhone as text) as text =>
    let
        Stripped = Text.Remove(rawPhone, {"-", "(", ")", " ", "."}),
        Formatted = if Text.Length(Stripped) = 10
                    then "(" & Text.Start(Stripped, 3) & ") " &
                         Text.Middle(Stripped, 3, 3) & "-" &
                         Text.End(Stripped, 4)
                    else rawPhone
    in
        Formatted,

// Use the function in a column
CleanedNumbers = Table.TransformColumns(ContactsTable,
    {{"Phone", CleanPhoneNumber, type text}})
```

### Combining Queries (Merge / Append)
```m
// Left Outer Join
MergedQuery = Table.NestedJoin(
    Orders, {"CustomerID"},
    Customers, {"CustomerID"},
    "CustomerDetails",
    JoinKind.LeftOuter
),
Expanded = Table.ExpandTableColumn(
    MergedQuery,
    "CustomerDetails",
    {"CustomerName", "Email", "City"},
    {"Customer.Name", "Customer.Email", "Customer.City"}
),

// Append (Union) multiple queries
AllSales = Table.Combine({Sales2022, Sales2023, Sales2024})
```

---

## End-to-End Tutorial: Dataflow Gen2 + Pipeline Together

This tutorial builds a **Medallion architecture** (Bronze → Silver → Gold) using both tools.

### Architecture
```
[Source: SQL Server]
        │
        ▼
[Pipeline: Copy Activity]  ──── Bronze Lakehouse (raw .parquet files)
        │
        ▼
[Pipeline: Dataflow Activity] → Runs Dataflow Gen2 (Silver transformation)
        │                         ├─ Filter nulls
        │                         ├─ Rename columns
        │                         ├─ Join with dimension tables
        │                         └─ Output → Silver Lakehouse (Delta tables)
        ▼
[Pipeline: Notebook Activity] ── Gold aggregation (Spark)
        │
        ▼
[Pipeline: Semantic Model Refresh] → Power BI report refreshed
```

### Step 1: Create the Dataflow Gen2 (Silver Layer)

1. Go to your **Fabric Workspace** → **New** → **Dataflow Gen2**
2. Name it: `DF_Silver_Sales`
3. **Get Data** → **Lakehouse** → select Bronze Lakehouse → select `raw_orders` table
4. Apply transformations:

```m
let
    // Step 1: Source from Bronze Lakehouse
    Source = Lakehouse.Contents(null){[workspaceId="your-ws-id",
                                       artifactId="your-lh-id"]}[Data],
    Orders = Source{[Schema="dbo",Item="raw_orders"]}[Data],

    // Step 2: Remove rows where OrderID is null
    RemoveNulls = Table.SelectRows(Orders, each [OrderID] <> null),

    // Step 3: Rename columns to snake_case standard
    Renamed = Table.RenameColumns(RemoveNulls, {
        {"OrderID", "order_id"},
        {"CustomerID", "customer_id"},
        {"OrderDate", "order_date"},
        {"TotalAmount", "total_amount"},
        {"ProductID", "product_id"}
    }),

    // Step 4: Cast data types
    TypedColumns = Table.TransformColumnTypes(Renamed, {
        {"order_id", Int64.Type},
        {"customer_id", Int64.Type},
        {"order_date", type date},
        {"total_amount", type number}
    }),

    // Step 5: Add derived columns
    WithYear = Table.AddColumn(TypedColumns, "order_year",
        each Date.Year([order_date]), Int64.Type),
    WithMonth = Table.AddColumn(WithYear, "order_month",
        each Date.Month([order_date]), Int64.Type),

    // Step 6: Filter test orders
    CleanData = Table.SelectRows(WithMonth,
        each [total_amount] > 0 and [order_id] > 0)
in
    CleanData
```

5. Right-click query → **Add data destination** → **Lakehouse** → Silver Lakehouse → Table: `silver_orders`
6. Choose: **Replace** (full load) or **Append** (incremental)
7. Click **Publish**

---

### Step 2: Create the Fabric Pipeline

1. Workspace → **New** → **Data pipeline** → Name: `PL_Medallion_Sales`

2. **Activity 1 — Copy Activity (Bronze Ingestion)**:
   - Source: SQL Server (via Connection)
   - Query: `SELECT * FROM dbo.Orders WHERE ModifiedDate >= '@{pipeline().parameters.LastRunDate}'`
   - Sink: Bronze Lakehouse → Files/raw/orders/`@{formatDateTime(utcNow(), 'yyyyMMdd')}`/orders.parquet

3. **Activity 2 — Dataflow Activity (Silver Transform)**:
   - Connect from Copy Activity → **On Success**
   - Settings: Workspace = your workspace, Dataflow = `DF_Silver_Sales`

4. **Activity 3 — Notebook Activity (Gold Layer)**:
   - Connect from Dataflow → **On Success**
   - Select Fabric Notebook: `NB_Gold_Aggregations`
   - Pass parameters: `{"run_date": "@{pipeline().parameters.LastRunDate}"}`

5. **Activity 4 — Semantic Model Refresh**:
   - Connect from Notebook → **On Success**
   - Refresh your Power BI Semantic Model automatically

6. **Add Error Handling**:
   - Add **Fail** activity connected via **On Failure** from any critical step
   - Add **Web** activity to POST to a Teams webhook on failure

7. **Add Schedule Trigger**:
   - Home → Schedule → Daily at 6:00 AM UTC

---

### Step 3: Run & Monitor

```
Fabric Workspace → Monitor → All Runs → Filter: Pipeline = PL_Medallion_Sales

Pipeline Run View:
  ✅ CopyActivity_Bronze     | Duration: 00:02:34 | Rows: 125,432
  ✅ DataflowActivity_Silver | Duration: 00:04:12 | Status: Succeeded
  ✅ NotebookActivity_Gold   | Duration: 00:01:58 | Status: Succeeded
  ✅ RefreshSemanticModel    | Duration: 00:00:45 | Status: Succeeded

Total Duration: 00:09:29 | Status: Succeeded
```

---

## Copilot AI Integration

### Copilot in Dataflow Gen2

Copilot understands natural language to generate M code [web:22]:

**Prompt examples:**
```
"Only keep rows where the sales amount is above the median value"
→ Generated M: Table.SelectRows(Source, each [SalesAmount] > 
    List.Median(Source[SalesAmount]))

"Count the total number of employees by City"
→ Generated M: Table.Group(Source, {"City"}, 
    {{"EmployeeCount", each Table.RowCount(_), Int64.Type}})

"Only keep European customers"
→ Generated M: Table.SelectRows(Source, each 
    List.Contains({"UK","France","Germany","Italy","Spain"}, [Country]))
```

**How to use Copilot in Dataflow Gen2:**
1. Open Dataflow Gen2 editor
2. Click **Copilot** button in the ribbon
3. Type your transformation request in plain English
4. Review generated M code and applied step
5. Accept or modify as needed

---

## Monitoring & Debugging

### Dataflow Gen2 Monitoring

**Refresh History** (per dataflow):
```
Dataflow Gen2 → Refresh History
  Run 1: 2024-01-15 06:00 | Duration: 00:04:12 | Status: ✅ Success | Rows: 45,231
  Run 2: 2024-01-14 06:00 | Duration: 00:03:58 | Status: ✅ Success | Rows: 44,876
  Run 3: 2024-01-13 06:00 | Duration: 00:08:43 | Status: ❌ Failed  | Error: Timeout
```

**Monitoring Hub**:
- Fabric Workspace → Monitor icon → filter by Type: Dataflow
- Cross-workspace visibility (Admin access)

**Debugging tips:**
- Use **Data preview** at each step to spot issues early
- Check **Diagram View** to validate merge relationships
- Use `Table.RowCount(Source)` in a new query to count source rows before transformation
- Temporarily disable output destination to test transformations without writing data

---

### Pipeline Monitoring

**Pipeline Run Details**:
```
Monitor → Pipeline Runs → Click Run ID

Activity Timeline:
  [Copy]──(2m 34s)──[Dataflow]──(4m 12s)──[Notebook]──(1m 58s)──[Refresh]──(45s)
                    ↑ Click to see:
                    - Rows read: 125,432
                    - Rows written: 124,899
                    - Data read: 2.4 GB
                    - Duration breakdown: Queue(5s) | Pre-copy(12s) | Copy(2m17s)
```

**Debugging tips:**
- Use **Interactive Run** (not schedule) for testing — faster feedback.
- Add **Set Variable** activity to log intermediate values.
- Use `@activity('ActivityName').output.rowsCopied` to pass row counts to downstream activities.
- Use **Web** activity to call a logging endpoint on failure.

---

## Decision Framework

Use this flowchart to decide which tool to use:

```
START: What do I need to do?
        │
        ├── Move large data with minimal transformation?
        │       └── Use PIPELINE (Copy Activity)
        │
        ├── Transform / clean / enrich data (business logic)?
        │       ├── Complex Spark-level logic?
        │       │       └── Use NOTEBOOK (Spark)
        │       └── Low-medium complexity, visual preferred?
        │               └── Use DATAFLOW GEN2
        │
        ├── Sequence multiple steps with dependencies?
        │       └── Use PIPELINE (orchestrate Dataflows + Notebooks)
        │
        ├── React to an event (file arrival, data threshold)?
        │       └── Use PIPELINE (with Storage Event or Activator trigger)
        │
        └── Refresh a Power BI Semantic Model after ETL?
                └── Use PIPELINE (with Semantic Model Refresh activity)
```

### Microsoft's Official Guidance [web:30]

| Tool | Data Volume | Transformation Complexity | Primary Persona |
|---|---|---|---|
| **Pipeline Copy Activity** | Low → High (PBs) | Low (column mapping, type cast) | Data Engineer |
| **Dataflow Gen2** | Low → High | Low → High (300+ functions) | Engineer + Analyst |
| **Spark Notebook** | Low → High | Low → High (full Python/Scala/SQL) | Data Engineer / Scientist |
| **Eventstream** | Medium → High | Low | Data Engineer |

---

## Best Practices

### Dataflow Gen2 Best Practices
1. **Limit query steps** — Each step is a Spark operation. Fewer steps = better performance.
2. **Use Query Folding** — When Power Query can push operations to the source database (SQL pushdown), it's much faster. Check via: right-click step → "View Native Query".
3. **Prefer Delta tables** as output destination for incremental refresh compatibility.
4. **Use Append mode** for time-series data; **Replace mode** for dimension tables.
5. **Name queries descriptively** — `raw_orders`, `clean_customers`, `merged_sales` not Query1, Query2.
6. **Disable staging** for small datasets to reduce overhead (Advanced → Staging disabled).
7. **Use parameters** for any hardcoded values (dates, schema names, server names).
8. **Import from Power Query template** to reuse transformation patterns across dataflows.

### Fabric Pipeline Best Practices
1. **Use parameters** for all hardcoded values (dates, table names, file paths).
2. **Always add On Failure paths** from critical activities to a logging or alerting step.
3. **Use ForEach with parallelism** for processing multiple tables — set `batchCount` wisely (3-10).
4. **Chain activities properly**: Copy → Dataflow → Notebook → Semantic Refresh (in order).
5. **Use Lookup activity** before Copy to check if source has new data before running expensive operations.
6. **Log run metadata** — Store pipeline run ID, start time, row counts in a logging table.
7. **Test triggers** in development workspace before applying to production.
8. **Use Deployment Pipelines** (Dev → Test → Prod) for safe promotion of pipeline changes.

---

*References: [Microsoft Fabric Docs](https://learn.microsoft.com/en-us/fabric/data-factory/) | [Fabric Decision Guide](https://learn.microsoft.com/en-us/fabric/fundamentals/decision-guide-pipeline-dataflow-spark) | Last reviewed: March 2026*
