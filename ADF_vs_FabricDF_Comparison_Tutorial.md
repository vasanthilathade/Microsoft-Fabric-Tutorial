# Azure Data Factory (ADF) vs. Fabric Data Factory (FDF)
> A detailed compare-and-contrast tutorial for data engineers

---

## Table of Contents
1. [Overview](#overview)
2. [Architecture Philosophy](#architecture-philosophy)
3. [Core Feature Comparison](#core-feature-comparison)
4. [Pipeline Orchestration](#pipeline-orchestration)
5. [Data Transformation](#data-transformation)
6. [Connectivity Model](#connectivity-model)
7. [Integration Runtime vs. On-Premises Data Gateway](#integration-runtime-vs-on-premises-data-gateway)
8. [CI/CD & Version Control](#cicd--version-control)
9. [Monitoring & Debugging](#monitoring--debugging)
10. [Security & Governance](#security--governance)
11. [Pricing Model](#pricing-model)
12. [Activity Comparison](#activity-comparison)
13. [When to Use Each](#when-to-use-each)
14. [Migration Tips (ADF → FDF)](#migration-tips-adf--fdf)

---

## Overview

| | Azure Data Factory (ADF) | Fabric Data Factory (FDF) |
|---|---|---|
| **Service Type** | PaaS (Platform as a Service) | SaaS (Software as a Service) |
| **Released** | 2015 (GA) | 2023 (part of Microsoft Fabric) |
| **Primary Use Case** | Enterprise ETL/ELT at scale, hybrid data integration | Unified analytics inside Microsoft Fabric ecosystem |
| **Built On** | Azure cloud services | Microsoft Fabric / OneLake |
| **Target Audience** | Data engineers, IT pros with complex pipelines | Data engineers + analysts seeking simplified, modern workflows |

> 💡 **Key Insight**: Fabric Data Factory is the **next generation of ADF**, rebuilt as a SaaS service deeply embedded in the Microsoft Fabric ecosystem. Microsoft is actively converging the two, but ADF remains the more mature, enterprise-battle-tested option.

---

## Architecture Philosophy

### Azure Data Factory
- **PaaS model**: You manage integration runtimes, datasets, linked services, and publish cycles.
- Designed for **maximum flexibility** — supports hybrid, multi-cloud, and on-premises data movement.
- Works **standalone** as an Azure resource, deployable via ARM templates.
- Requires explicit **Publish** step before changes go live.

### Fabric Data Factory
- **SaaS model**: Microsoft manages the compute infrastructure — no integration runtime provisioning needed.
- Designed for **simplicity and native integration** within Fabric workspaces (Lakehouse, Warehouse, Power BI, OneLake).
- Works inside a **Fabric workspace** — no separate resource provisioning.
- **No publish step** — just Save or Run. Changes are reflected immediately.

---

## Core Feature Comparison

| Feature | ADF | FDF | Notes |
|---|---|---|---|
| Pipeline Orchestration | ✅ Full-featured | ✅ Full-featured | FDF pipelines unified with Lakehouse/Warehouse |
| Transformations | Mapping Dataflows | Dataflow Gen2 (Power Query) | FDF is easier for low-code transformations |
| Connectivity | Datasets + Linked Services | Connections only | FDF simplified — no dataset objects |
| Integration Runtime | Azure IR / Self-hosted IR | Fabric-managed compute | No manual IR setup in FDF |
| On-Premises Connectivity | Self-hosted IR (SHIR) | On-Premises Data Gateway (OPDG) | Different agent, similar purpose |
| Triggers | Schedule, Tumbling Window, Event-based | Schedule, File Event, Activator (Reflex) | Expanding in FDF |
| Publish Cycle | Explicit Publish required | Save & Run (no publish step) | FDF is faster to iterate |
| CI/CD | ARM templates + Azure DevOps/GitHub | Built-in Deployment Pipelines + Git repos | FDF offers easier cherry-picking |
| Monitoring | ADF Studio + Log Analytics | Unified Monitoring Hub | FDF provides cross-workspace views |
| Debugging | Debug Mode (toggle) | Always-on Interactive Mode | FDF removes debug toggle complexity |
| Copilot / AI | ❌ Not available | ✅ Copilot integrated | Natural language pipeline creation in FDF |
| SSIS Integration | ✅ Azure-SSIS IR | 🔜 TBD | Not yet in FDF |
| Managed VNet | ✅ Supported | 🔜 TBD | Roadmap for FDF |
| Change Data Capture (CDC) | ✅ CDC artifacts | ✅ Copy Jobs (incremental) | Different implementation |
| Synapse Link | ✅ Azure Synapse Link | ✅ Mirroring | FDF uses Mirroring instead |

---

## Pipeline Orchestration

### ADF Pipelines
```json
// Example: ADF Linked Service (SQL Server)
{
  "name": "SqlServerLinkedService",
  "type": "Microsoft.DataFactory/factories/linkedservices",
  "properties": {
    "type": "SqlServer",
    "typeProperties": {
      "connectionString": "Server=myserver;Database=mydb;...",
      "password": {
        "type": "AzureKeyVaultSecret",
        "store": { "referenceName": "AzureKeyVault", "type": "LinkedServiceReference" },
        "secretName": "sql-password"
      }
    }
  }
}
```
- Uses **Linked Services** + **Datasets** as separate objects.
- Pipeline triggers include: Schedule, Tumbling Window, Storage Event, Custom Event.
- Requires **Publish** before pipelines are live.
- Full ARM template export for IaC deployments.

### FDF Pipelines
```json
// FDF: Connections replace Linked Services + Datasets
// Data source defined INLINE within the Copy Activity itself
{
  "name": "CopyFromSQL",
  "type": "Copy",
  "inputs": [{ "referenceName": "SqlConnection", "type": "ConnectionReference" }],
  "source": {
    "type": "SqlSource",
    "sqlReaderQuery": "SELECT * FROM SalesData"
  }
}
```
- **No separate Dataset objects** — source/sink defined inline in each activity.
- Connections replace Linked Services — simpler setup.
- **Invoke Pipeline** activity (vs. Execute Pipeline in ADF) supports cross-platform invocation.
- Save & Run immediately — no publish cycle.

---

## Data Transformation

### ADF: Mapping Dataflows
- Apache Spark-based visual transformation engine.
- Ideal for complex, large-scale transformations.
- Runs on Azure IR with configurable compute (core count, TTL).
- Supports schema drift, parameterization, and debugging.

```
Source (SQL DB)
  → Select (columns)
    → Derived Column (calculated fields)
      → Aggregate (GROUP BY)
        → Sink (Azure Data Lake Gen2)
```

### FDF: Dataflow Gen2 (Power Query Based)
- Built on **Power Query** — the same engine used in Excel, Power BI, and Power Apps.
- Easier for data analysts, not just engineers.
- Directly outputs to **OneLake, Lakehouse, Warehouse**.
- No compute configuration needed — Fabric manages it.

```
// Power Query M Language example in Dataflow Gen2
let
    Source = Sql.Database("myserver", "mydb"),
    SalesTable = Source{[Schema="dbo",Item="Sales"]}[Data],
    FilteredRows = Table.SelectRows(SalesTable, each [Year] = 2024),
    GroupedRows = Table.Group(FilteredRows, {"Region"}, {{"TotalSales", each List.Sum([Amount]), type number}})
in
    GroupedRows
```

> **Tip for Data Engineers**: If you're comfortable with Spark-level control, ADF Mapping Dataflows give more power. If you work closely with Power BI analysts, FDF Dataflow Gen2 bridges the gap beautifully.

---

## Connectivity Model

### ADF: Linked Services + Datasets
```
Pipeline
 └── Activity
      ├── Source Dataset → Linked Service (Connection String / Auth)
      └── Sink Dataset   → Linked Service (Connection String / Auth)
```
- Datasets are **reusable objects** — define schema, file format, and path separately.
- Linked Services handle auth (Managed Identity, Service Principal, Key Vault secrets).
- 100+ connectors: Azure, AWS S3, GCP, SAP, Salesforce, REST APIs, and more.

### FDF: Connections
```
Pipeline
 └── Activity
      └── Inline Source/Sink definition → Connection (auth only)
```
- No Dataset object — source/sink configs live **inside the activity**.
- Connections are shared across the Fabric tenant — reuse across workspaces.
- **Get Data** experience streamlines new connection creation.
- Same broad connector support as ADF.

---

## Integration Runtime vs. On-Premises Data Gateway

| Feature | ADF: Self-Hosted IR (SHIR) | FDF: On-Premises Data Gateway (OPDG) |
|---|---|---|
| Purpose | Connect to on-prem / private network data | Connect to on-prem / private network data |
| Registration | Key-based | Microsoft Entra ID account |
| Platform | Windows + Container | Windows only |
| HA Nodes | Up to 8 nodes | Up to 10 nodes |
| Region Binding | Fixed to ADF region | Flexible, can change region |
| Auto-update | ✅ Supported | ❌ Not supported |
| Credential Store | Local on SHIR + Key Vault | Centralized in Gateway cloud service |
| Sharing | Up to 120 Data Factories | Available to all Fabric services in tenant |

> **Note for migration**: When moving from ADF to FDF, you must replace SHIR with the **On-Premises Data Gateway**. They are separate agents and cannot be shared.

---

## CI/CD & Version Control

### ADF CI/CD
```
Dev ADF (Git-connected)
  → Feature branch → PR to main
    → Release pipeline (ARM template export)
      → Deploy to Test ADF
        → Deploy to Prod ADF
```
- CI/CD tightly coupled to **ARM templates**.
- Must connect to an external Git repo (Azure DevOps or GitHub).
- All-or-nothing workspace promotion — harder to cherry-pick individual items.
- Manual pipeline configuration in Azure DevOps/GitHub Actions.

### FDF CI/CD
```
Dev Workspace (Git-connected OR standalone)
  → Fabric Deployment Pipeline
    → Test Workspace
      → Prod Workspace
```
- Two options: **Built-in Fabric Deployment Pipelines** (no external Git required) or external Git repo.
- Easy **cherry-picking** — promote individual pipelines, dataflows, lakehouses.
- No ARM templates — cleaner separation of concerns.
- **Workspace-level RBAC** controls who can promote to prod.

```bash
# FDF also supports external Git integration for advanced teams:
# Connect workspace → Settings → Git Integration → Azure DevOps / GitHub
```

---

## Monitoring & Debugging

### ADF Monitoring
- Dedicated **Monitor tab** in ADF Studio.
- Pipeline runs, trigger runs, activity runs — all filterable.
- Integration with **Azure Monitor** and **Log Analytics** for alerting.
- Detailed activity-level diagnostics.

```
ADF Studio → Monitor → Pipeline Runs → Click Run → Activity Details
```

### FDF Monitoring Hub
- **Unified Monitoring Hub** across all Fabric workloads (pipelines, notebooks, dataflows, warehouses).
- **Cross-workspace visibility** — see all runs in your organization.
- Built-in **Run History** per item.
- **Duration Breakdown** view for Copy Activities — shows time spent per stage (queue, transfer, post-process).

```
Fabric Workspace → Monitor → All Runs → Filter by Type: Pipeline
```

> **Debugging difference**: ADF has a toggle "Debug Mode" that runs pipelines in debug mode with breakpoints. FDF is **always in interactive mode** — no mode switching needed. This makes iteration faster.

---

## Security & Governance

### ADF Security
- **Managed Identity** for Azure-to-Azure auth (recommended).
- **Service Principal** for cross-tenant or automation scenarios.
- **Azure Key Vault** for secret management.
- Private endpoints + Managed VNet for network isolation.
- Azure RBAC roles (Contributor, Reader, Data Factory Contributor).

### FDF Security
- All ADF auth types supported (Managed Identity, Service Principal, OAuth2).
- **Fabric Workspace RBAC**: Admin, Member, Contributor, Viewer roles.
- **OneLake security** — row-level and column-level security for Lakehouse data.
- Virtual Network Data Gateway for private connectivity (customer-managed VNet).
- Secrets managed via workspace-level connections (Key Vault integration planned).

---

## Pricing Model

### ADF Pricing (Pay-as-you-go)
| Component | Cost Driver |
|---|---|
| Pipeline Activity Runs | Per activity execution |
| Data Movement | Per DIU (Data Integration Unit) hour |
| Mapping Dataflows | Per vCore-hour (Spark compute) |
| Self-hosted IR | Infrastructure cost (VM you manage) |
| Azure IR (Managed VNet) | Premium per vCore-hour |

### FDF Pricing (Capacity-based)
| Component | Cost Driver |
|---|---|
| Fabric Capacity (F SKU) | Monthly/hourly Capacity Unit (CU) reservation |
| Pipeline Activity Runs | CU consumption |
| Dataflow Gen2 | CU consumption |
| External Pipeline Activities | **No charge** for triggering external services |
| OneLake Storage | Per GB/month |

> **Cost tip**: FDF's capacity model is **more predictable** for steady-state workloads. ADF's pay-as-you-go can be cheaper for sporadic, low-volume pipelines. Evaluate based on your pipeline frequency and data volume.

---

## Activity Comparison

| Activity | ADF | FDF |
|---|---|---|
| Copy Data | ✅ | ✅ |
| Dataflow (Mapping) | ✅ Mapping Dataflow | ✅ Dataflow Gen2 |
| Notebook | Azure Databricks Notebook | ✅ Fabric Notebook (native) |
| Script | ✅ | ✅ |
| Stored Procedure | ✅ | ✅ |
| For Each / If Condition / Switch | ✅ | ✅ |
| Execute/Invoke Pipeline | Execute Pipeline | Invoke Pipeline (cross-platform) |
| Get Metadata | ✅ | ✅ |
| Lookup | ✅ | ✅ |
| Web / Webhook | ✅ | ✅ |
| Azure Batch | ✅ | ✅ |
| Azure ML | ✅ | ✅ |
| SSIS | ✅ Azure-SSIS IR | ❌ Not yet available |
| Office 365 Outlook | ❌ | ✅ New in FDF |
| Microsoft Teams | ❌ | ✅ New in FDF |
| Semantic Model Refresh | ❌ | ✅ New in FDF |
| Synapse Notebook | ✅ | ❌ N/A |
| HDInsight | ✅ (Hive/MapReduce/Spark) | ✅ (unified HDInsight activity) |
| Fail | ✅ | ✅ |
| Delete | ✅ | ✅ |

> ~90% of ADF activities are available in FDF today, with Microsoft adding parity continuously.

---

## When to Use Each

### ✅ Choose Azure Data Factory when:
- You have **complex, enterprise-grade pipelines** already in ADF.
- You need **SSIS integration** (Azure-SSIS IR).
- You require **Managed Virtual Network** / private endpoints.
- You work in **multi-cloud or hybrid** environments (AWS S3, GCP BigQuery, SAP).
- Your team has deep ADF expertise and existing ARM template-based CI/CD.
- You need **fine-grained compute control** for Spark-based Mapping Dataflows.
- Your org is **not yet on Microsoft Fabric**.

### ✅ Choose Fabric Data Factory when:
- You are **adopting Microsoft Fabric** (Lakehouse, Warehouse, Power BI, OneLake).
- You want a **simplified, SaaS experience** — no IR management.
- Your team includes **business analysts** who prefer Power Query over Spark.
- You want **native Copilot AI assistance** for pipeline creation.
- You need **cross-workspace monitoring** and unified governance.
- You prefer **built-in CI/CD** without setting up external ARM-based pipelines.
- You want native integration with **Fabric Notebooks, Semantic Models, and Teams/Outlook**.

### 🔄 Hybrid Approach (Recommended for Many Enterprises)
Many organizations run **both simultaneously**:
- **ADF** → Legacy and complex hybrid workloads, SSIS, and established pipelines.
- **FDF** → New Fabric-native projects, Power BI-connected pipelines, and analyst-facing workflows.

---

## Migration Tips (ADF → FDF)

1. **Audit existing ADF assets**: Catalog all pipelines, linked services, datasets, dataflows, triggers, and IRs.
2. **Identify blockers**: SSIS pipelines, Managed VNet dependencies, and ARM-heavy CI/CD need planning.
3. **Replace SHIR with OPDG**: Install the On-Premises Data Gateway on the same or equivalent machine.
4. **Rebuild Linked Services as Connections**: FDF connections are simpler — migrate auth configs.
5. **Convert Datasets to inline definitions**: Remove dataset objects; move schema/path config into activities.
6. **Migrate Mapping Dataflows → Dataflow Gen2**: Rebuild transformations in Power Query M; test outputs.
7. **Set up Fabric Workspace CI/CD**: Choose built-in Deployment Pipelines or connect to Git repo.
8. **Validate monitoring parity**: Set up alerts in Fabric Monitoring Hub equivalent to ADF Azure Monitor alerts.
9. **Run parallel validation**: Run ADF and FDF pipelines in parallel and compare row counts and output schemas.
10. **Cutover incrementally**: Migrate by domain or pipeline group, not all at once.

> 📖 **Reference**: [Microsoft official migration guide](https://learn.microsoft.com/en-us/fabric/data-factory/migrate-planning-azure-data-factory)

---

## Quick Reference Cheat Sheet

| Concept in ADF | Equivalent in FDF |
|---|---|
| Linked Service | Connection |
| Dataset | Inline activity definition |
| Mapping Dataflow | Dataflow Gen2 |
| Azure Integration Runtime | Fabric-managed compute |
| Self-hosted IR (SHIR) | On-Premises Data Gateway (OPDG) |
| Publish | Save (auto-save on Run) |
| Execute Pipeline activity | Invoke Pipeline activity |
| Debug Mode | Interactive Mode (always on) |
| ARM template export | Save As / Git integration |
| Azure Monitor / Log Analytics | Fabric Monitoring Hub |
| Azure Synapse Link | Mirroring |
| CDC artifacts | Copy Jobs |

---

*Generated with data from [Microsoft Docs](https://learn.microsoft.com/en-us/fabric/data-factory/compare-fabric-data-factory-and-azure-data-factory) | Last reviewed: March 2026*
