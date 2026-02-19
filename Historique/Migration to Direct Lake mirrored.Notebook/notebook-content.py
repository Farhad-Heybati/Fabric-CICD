# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00dad5e5-d945-4e22-8ba4-49fbc9845d98",
# META       "default_lakehouse_name": "demo_databricks_v4",
# META       "default_lakehouse_workspace_id": "9d662f4d-ad03-4c16-82a4-8db29b422812",
# META       "known_lakehouses": [
# META         {
# META           "id": "bc5f1704-946b-4417-99e3-ed466d2a558d"
# META         },
# META         {
# META           "id": "00dad5e5-d945-4e22-8ba4-49fbc9845d98"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### Watch thie video below to see a walkthrough of the Direct Lake Migration process
# [![Direct Lake Migration Video](https://img.youtube.com/vi/gGIxMrTVyyI/0.jpg)](https://www.youtube.com/watch?v=gGIxMrTVyyI?t=495)


# MARKDOWN ********************

# ### Install the latest .whl package
# 
# Check [here](https://pypi.org/project/semantic-link-labs/) to see the latest version.

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Import the library and set initial parameters

# CELL ********************

import sempy_labs as labs
from sempy_labs import migration, directlake
import sempy_labs.report as rep

dataset_name = 'poc-aas-direct-query-adb' #Enter the import/DQ semantic model name
workspace_name = None #Enter the workspace of the import/DQ semantic model. It set to none it will use the current workspace.
new_dataset_name = 'poc-aas-direct-lake-adb-v4' #Enter the new Direct Lake semantic model name
new_dataset_workspace_name = None #Enter the workspace where the Direct Lake model will be created. If set to None it will use the current workspace.
lakehouse_name = "demo_databricks_v4" #Enter the lakehouse to be used for the Direct Lake model. If set to None it will use the lakehouse attached to the notebook.
lakehouse_workspace_name = None #Enter the lakehouse workspace. If set to None it will use the new_dataset_workspace_name.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Create the [Power Query Template](https://learn.microsoft.com/power-query/power-query-template) file
# 
# This encapsulates all of the semantic model's Power Query logic into a single file.

# CELL ********************

migration.create_pqt_file(dataset = dataset_name, workspace = workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


import sempy.fabric as fabric

workspace_name = "Databricks-Gold-Sales-Customers-Products"  # your workspace
items_df = fabric.list_items(type="Lakehouse", workspace=workspace_name)

print(items_df[["Display Name", "Type"]])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ============================================
# DirectQuery -> Direct Lake (on SQL) migration
# Mirrored Azure Databricks SQL endpoint
# ============================================

# 1) Install & import libraries
%pip install -q semantic-link-labs

import sempy_labs as labs
import sempy.fabric as fabric
from sempy_labs import migration, directlake

# 2) ---- YOUR SETTINGS ----
# Existing Import/DirectQuery model
dq_dataset_name       = "SalesDQ"                   # <-- your current DQ model name
dq_workspace_name     = "Integration-Darabricks"    # workspace containing the DQ model

# New Direct Lake model
dl_dataset_name       = "Sales_DirectLake_SQL"      # name for the new DL model
dl_workspace_name     = "Integration-Darabricks"    # workspace for the new DL model

# Mirrored Databricks endpoint details
# Option A: If you KNOW the endpoint host (recommended):
endpoint_host         = "nb2xzbxv2vcutdntxeesvrr3vq-juxwnhidvulezaverwzjwqrici.datawarehouse.fabric.microsoft.com"
endpoint_workspace    = "Integration-Darabricks"    # the workspace containing the mirrored item/endpoint

# Option B: If you DON’T have the host, but know the Mirrored item name:
mirrored_item_name    = None                        # e.g., "Contoso_UC_Mirrored"; set to None if using host above

# Staging Lakehouse (only used to materialize PQ transforms & to scaffold DL model)
# If your DQ model uses M transforms not supported in Direct Lake, we materialize them via Dataflows Gen2 into this Lakehouse.
staging_lakehouse_name       = "lh_staging_bridge"
staging_lakehouse_workspace  = dl_workspace_name

# Control flags
export_powerquery_template   = True   # export .pqt from DQ model for Dataflows Gen2
generate_dl_from_staging     = True   # use staging lakehouse tables to create DL model before rebinding to SQL endpoint
strict_no_views              = True   # warn/error if any listed entities are views (DL on SQL will fall back on views)

# 4) (Optional) Ensure the staging Lakehouse exists (to host materialized tables and scaffold the DL model)
#    You can create the lakehouse via UI; here we only check existence programmatically.
print("\nChecking for staging Lakehouse...")
staging_items_df = fabric.list_items(type="Lakehouse", workspace=staging_lakehouse_workspace)
staging_lh_names = set(staging_items_df['Display Name'].tolist()) if staging_items_df is not None and len(staging_items_df)>0 else set()
if staging_lakehouse_name not in staging_lh_names:
    print(f"⚠️ Lakehouse '{staging_lakehouse_name}' not found in workspace '{staging_lakehouse_workspace}'.")
    print("   Create it in the UI (New → Lakehouse) or attach an existing one, then re-run.")
    # If you want to auto-create programmatically, uncomment the following line (requires notebookutils):
    # from notebookutils import lakehouse as nb_lh
    # nb_lh.create(name=staging_lakehouse_name, description="Staging bridge for DL migration", workspaceId=fabric.resolve_workspace_id(staging_lakehouse_workspace))
    raise SystemExit("Please create the staging Lakehouse and re-run.")
print("✅ Staging Lakehouse located.")

# 5) Discover materialized tables in the staging Lakehouse (after .pqt import to Dataflows Gen2)
staging_tables_df = labs.lakehouse.get_lakehouse_tables(
    lakehouse = staging_lakehouse_name,
    workspace = staging_lakehouse_workspace
)
# Robust detection of "table name" column
candidate_cols = [c for c in staging_tables_df.columns if c.lower() in ("name","table","table name","entity name")]
table_col = candidate_cols[0] if candidate_cols else None
staging_tables = staging_tables_df[table_col].tolist() if table_col else []

print(f"Found {len(staging_tables)} table(s) in staging lakehouse: {staging_tables[:10]}{'...' if len(staging_tables)>10 else ''}")

# 6) Create the new Direct Lake semantic model (scaffold), using staging lakehouse tables
#    This step ensures we have a DL model to bind later to the Mirrored Databricks SQL endpoint.
if generate_dl_from_staging:
    if not staging_tables:
        raise ValueError("No tables detected in the staging Lakehouse. Import the .pqt into Dataflows Gen2 and publish to create Delta tables.")
    print("\nCreating the new Direct Lake semantic model from staging Lakehouse tables...")
    directlake.generate_direct_lake_semantic_model(
        dataset             = dl_dataset_name,
        lakehouse_tables    = staging_tables,
        workspace           = dl_workspace_name,
        lakehouse           = staging_lakehouse_name,
        lakehouse_workspace = staging_lakehouse_workspace,
        overwrite           = True,
        refresh             = True
    )
    print("✅ Direct Lake model created (scaffold).")
# Function reference: sempy_labs.directlake.generate_direct_lake_semantic_model. [1](https://pypi.org/project/semantic-link-labs/)

# 7) Resolve the Mirrored Databricks SQL endpoint owner item (Warehouse/Lakehouse) by host OR Mirrored name
print("\nResolving SQL endpoint owner item...")
source_item_name = None
source_item_type = None  # "Warehouse" or "Lakehouse"

try:
    # Preferred: list SQL endpoints in the workspace and match the connection string host
    from sempy.fabric import sql_endpoint
    endpoints_df = sql_endpoint.list_sql_endpoints(workspace=endpoint_workspace)
    # Guess the connection-string column
    host_cols = [c for c in endpoints_df.columns if c.lower().replace(" ", "") in ("connectionstring","endpoint","sqlep_connectionstring")]
    cs_col = host_cols[0] if host_cols else None
    if endpoint_host and cs_col:
        match_df = endpoints_df[endpoints_df[cs_col].str.contains(endpoint_host, case=False, na=False)]
        if len(match_df) > 0:
            # Prefer columns that reveal owning item name/type
            name_cols = [c for c in match_df.columns if c.lower() in ("item name","display name","name","warehouse name","lakehouse name")]
            type_cols = [c for c in match_df.columns if c.lower() in ("item type","type")]
            source_item_name = match_df.iloc[0][name_cols[0]] if name_cols else None
            source_item_type = match_df.iloc[0][type_cols[0]] if type_cols else "Warehouse"
            print(f"✅ Found endpoint owner: {source_item_name} ({source_item_type})")
except Exception as e:
    print(f"⚠️ sql_endpoint listing failed or columns differ: {e}")

# Fallback: if you know the Mirrored item name, bind to it directly
# (Mirrored Azure Databricks items expose a SQL analytics endpoint usable by DL on SQL). [3](https://blog.fabric.microsoft.com/en-us/blog/unified-by-design-mirroring-azure-databricks-unity-catalog-in-microsoft-fabric-now-generally-available?ft=Dipti%20Borkar:author)
if source_item_name is None and mirrored_item_name:
    items_df = fabric.list_items(type="MirroredAzureDatabricks", workspace=endpoint_workspace)
    available_names = set(items_df["Display Name"].tolist()) if items_df is not None and len(items_df)>0 else set()
    if mirrored_item_name in available_names:
        source_item_name = mirrored_item_name
        source_item_type = "MirroredAzureDatabricks"
        print(f"✅ Using Mirrored item as source: {source_item_name} ({source_item_type})")
    else:
        raise ValueError(f"Mirrored item '{mirrored_item_name}' not found in workspace '{endpoint_workspace}'. Found: {sorted(available_names)}")

# If still unresolved, try Warehouse/Lakehouse items and read their endpoint properties via notebookutils
if source_item_name is None:
    print("Attempting Warehouse/Lakehouse discovery via workspace items...")
    wh_df = fabric.list_items(type="Warehouse", workspace=endpoint_workspace)
    lh_df = fabric.list_items(type="Lakehouse", workspace=endpoint_workspace)
    try:
        from notebookutils import sqlendpoint as nb_sql
        # Try Warehouses first
        if wh_df is not None and len(wh_df)>0:
            for _, row in wh_df.iterrows():
                display_name = row.get("Display Name") or row.get("Name")
                props = nb_sql.getWithProperties(name=display_name, workspaceId=fabric.resolve_workspace_id(endpoint_workspace))
                conn = props["properties"]["connectionString"]
                if endpoint_host in conn:
                    source_item_name = display_name
                    source_item_type = "Warehouse"
                    break
        # Then try Lakehouses (some tenants surface endpoint on a LH)
        if source_item_name is None and lh_df is not None and len(lh_df)>0:
            for _, row in lh_df.iterrows():
                display_name = row.get("Display Name") or row.get("Name")
                props = nb_sql.getWithProperties(name=display_name, workspaceId=fabric.resolve_workspace_id(endpoint_workspace))
                conn = props["properties"]["connectionString"]
                if endpoint_host in conn:
                    source_item_name = display_name
                    source_item_type = "Lakehouse"
                    break
    except Exception as e2:
        print(f"⚠️ notebookutils fallback failed: {e2}")

if source_item_name is None:
    raise LookupError("Could not resolve the SQL endpoint owner item. "
                      "Verify the endpoint host or provide the Mirrored item name.")

print(f"Resolved endpoint owner: {source_item_name} ({source_item_type})")

# 8) Rebind the DL model to the Mirrored Databricks SQL endpoint (Direct Lake on SQL)
print("\nRebinding the model to use the SQL endpoint (Direct Lake on SQL)...")
directlake.update_direct_lake_model_connection(
    dataset          = dl_dataset_name,
    workspace        = dl_workspace_name,
    source           = source_item_name,    # Warehouse/Lakehouse/Mirrored item name
    source_type      = source_item_type,    # "Warehouse" | "Lakehouse" | "MirroredAzureDatabricks"
    source_workspace = endpoint_workspace,
    use_sql_endpoint = True,                # <-- key: Direct Lake on SQL
    # tables        = ["FactSales","DimCustomer"]  # uncomment for multi-expression models to target specific tables
)
print("✅ Model now points to the Mirrored Databricks SQL analytics endpoint.")
# Function reference: sempy_labs.directlake.update_direct_lake_model_connection. [1](https://pypi.org/project/semantic-link-labs/)[2](https://www.fourmoo.com/2025/08/20/how-to-use-the-tabular-object-model-using-semantic-link-labs-in-a-microsoft-fabric-notebook/)

# 9) Confirm the binding & validate schema
print("\nConfirming the DL source...")
src = directlake.get_direct_lake_source(dataset=dl_dataset_name, workspace=dl_workspace_name)
print("Source info:", src)  # ('Warehouse'|'Lakehouse', name, endpoint id, workspace id)

print("\nComparing model schema against endpoint tables...")
directlake.direct_lake_schema_compare(dataset=dl_dataset_name, workspace=dl_workspace_name)

print("\nSyncing any missing columns into the model (add_to_model=True)...")
directlake.direct_lake_schema_sync(
    dataset      = dl_dataset_name,
    workspace    = dl_workspace_name,
    add_to_model = True
)
print("✅ Schema synchronized.")
# Reference: sempy_labs.directlake.compare/sync helpers. [1](https://pypi.org/project/semantic-link-labs/)

# 10) Check for DirectQuery fallback risks (and fix upstream)
print("\nChecking fallback reasons...")
fallback_df = directlake.check_fallback_reason(dataset=dl_dataset_name, workspace=dl_workspace_name)
print(fallback_df)
# Guidance: Avoid SQL views in DL on SQL; binary columns, DateTime keys in relationships, and type mismatches can also cause fallback. [4](https://community.fabric.microsoft.com/t5/Notebook-Gallery/Migration-to-Direct-Lake/m-p/4623004)

# 11) (Optional) Enable AutoSync & warm cache for performance
print("\nEnabling AutoSync & warming cache...")
directlake.set_autosync(dataset=dl_dataset_name, workspace=dl_workspace_name, enable=True)
directlake.warm_direct_lake_cache_isresident(dataset=dl_dataset_name, workspace=dl_workspace_name)

print("\n✅ Migration complete: DirectQuery → Direct Lake (on SQL) pointing to Mirrored Databricks SQL endpoint.")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Import the Power Query Template to Dataflows Gen2
# 
# - Open the [OneLake file explorer](https://www.microsoft.com/download/details.aspx?id=105222) and sync your files (right click -> Sync from OneLake)
# 
# - Navigate to your lakehouse. From this window, create a new Dataflows Gen2 and import the Power Query Template file from OneLake (OneLake -> Workspace -> Lakehouse -> Files...), and publish the Dataflows Gen2.
# 
# <div class="alert alert-block alert-info">
# <b>Important!</b> Make sure to create the Dataflows Gen2 from within the lakehouse window. That will ensure that all the tables automatically map to that lakehouse as the destination. Otherwise, you will have to manually map each table to its destination individually.
# </div>

# MARKDOWN ********************

# ### Create the Direct Lake model based on the import/DQ semantic model
# 
# Calculated columns are not migrated to the Direct Lake model as they are not supported in Direct Lake mode.

# CELL ********************

import time
labs.create_blank_semantic_model(dataset = new_dataset_name, workspace = new_dataset_workspace_name, overwrite=False)

migration.migrate_calc_tables_to_lakehouse(
    dataset=dataset_name,
    new_dataset=new_dataset_name,
    workspace=workspace_name,
    new_dataset_workspace=new_dataset_workspace_name,
    lakehouse=lakehouse_name,
    lakehouse_workspace=lakehouse_workspace_name
)
migration.migrate_tables_columns_to_semantic_model(
    dataset=dataset_name,
    new_dataset=new_dataset_name,
    workspace=workspace_name,
    new_dataset_workspace=new_dataset_workspace_name,
    lakehouse=lakehouse_name,
    lakehouse_workspace=lakehouse_workspace_name
)
migration.migrate_calc_tables_to_semantic_model(
    dataset=dataset_name,
    new_dataset=new_dataset_name,
    workspace=workspace_name,
    new_dataset_workspace=new_dataset_workspace_name,
    lakehouse=lakehouse_name,
    lakehouse_workspace=lakehouse_workspace_name
)
migration.migrate_model_objects_to_semantic_model(
    dataset=dataset_name,
    new_dataset=new_dataset_name,
    workspace=workspace_name,
    new_dataset_workspace=new_dataset_workspace_name
)
migration.migrate_field_parameters(
    dataset=dataset_name,
    new_dataset=new_dataset_name,
    workspace=workspace_name,
    new_dataset_workspace=new_dataset_workspace_name
)
time.sleep(2)
labs.refresh_semantic_model(dataset=new_dataset_name, workspace=new_dataset_workspace_name)
migration.refresh_calc_tables(dataset=new_dataset_name, workspace=new_dataset_workspace_name)
labs.refresh_semantic_model(dataset=new_dataset_name, workspace=new_dataset_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Show migrated/unmigrated objects

# CELL ********************

migration.migration_validation(
    dataset=dataset_name,
    new_dataset=new_dataset_name, 
    workspace=workspace_name, 
    new_dataset_workspace=new_dataset_workspace_name
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Fabric notebook cell (Python)
from sempy_labs import admin, directlake
import pandas as pd
import re

DATASET_NAME   = "poc-aas-direct-lake-adb-v3"
WORKSPACE_NAME = "Integration-Darabricks"  # make sure this is the exact workspace name

# ---------- helpers ----------
def df_ok(df: pd.DataFrame) -> bool:
    return isinstance(df, pd.DataFrame) and not df.empty

def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names to lower_snake_case
    return df.rename(columns={c: c.strip().lower().replace(" ", "_") for c in df.columns})

def norm_name(s: str) -> str:
    # Case-insensitive, ignore spaces/underscores/hyphens when comparing names
    return re.sub(r"[_\-\s]+", "", str(s)).casefold()

def pick_col(df: pd.DataFrame, candidates) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

# ---------- 0) Resolve workspace ID ----------
ws_raw = admin.list_workspaces()
if not df_ok(ws_raw):
    raise RuntimeError("No accessible workspaces returned by admin.list_workspaces().")

ws = norm_cols(ws_raw)  # expect columns like: id, name, state, type, capacity_id
name_col = pick_col(ws, ["name","display_name","workspace_name"])
id_col   = pick_col(ws, ["id","workspace_id"])

if name_col is None or id_col is None:
    raise KeyError(f"Unexpected workspace columns: {list(ws.columns)}")

ws["name_norm"] = ws[name_col].apply(norm_name)
ws_row = ws.loc[ws["name_norm"] == norm_name(WORKSPACE_NAME)]
if ws_row.empty:
    print("Available workspaces (for reference):", sorted(ws[name_col].unique()))
    raise ValueError(f"Workspace '{WORKSPACE_NAME}' not found.")
WORKSPACE_ID = ws_row.iloc[0][id_col]
print(f"Workspace resolved: {ws_row.iloc[0][name_col]} (id={WORKSPACE_ID})")

# ---------- 1) Find the dataset in this workspace ----------
# Different builds expose dataset listings differently:
# Use list_items(workspace=...) and filter by type (SemanticModel/Dataset)
items_raw = admin.list_items(workspace=WORKSPACE_ID)
items = norm_cols(items_raw)
if not df_ok(items):
    raise RuntimeError(f"No items returned for workspace id {WORKSPACE_ID}.")

type_col = pick_col(items, ["type","item_type"])
disp_col = pick_col(items, ["display_name","name","item_name"])
item_id_col = pick_col(items, ["id","item_id"])
if type_col is None or disp_col is None or item_id_col is None:
    raise KeyError(f"Unexpected item columns: {list(items.columns)}")

ds_rows = items.loc[items[type_col].astype(str).str.contains("SemanticModel|Dataset", case=False, regex=True)]
if ds_rows.empty:
    raise RuntimeError("No datasets/semantic models found in this workspace.")

ds_rows["name_norm"] = ds_rows[disp_col].apply(norm_name)
ds_row = ds_rows.loc[ds_rows["name_norm"] == norm_name(DATASET_NAME)]
if ds_row.empty:
    print("Available datasets/semantic models:", sorted(ds_rows[disp_col].unique()))
    raise ValueError(f"Dataset '{DATASET_NAME}' not found in workspace '{WORKSPACE_NAME}'.")

DATASET_ID   = ds_row.iloc[0][item_id_col]
DATASET_NAME_RESOLVED = ds_row.iloc[0][disp_col]
print(f"Dataset resolved: {DATASET_NAME_RESOLVED} (id={DATASET_ID})")

# ---------- 2) Verify Direct Lake source (Lakehouse vs Warehouse) ----------
# Prefer IDs to avoid name mismatches.
src_raw = directlake.get_direct_lake_source(dataset=DATASET_ID, workspace=WORKSPACE_ID)
if not df_ok(src_raw):
    # If this is empty, the model is not Direct Lake OR binding couldn't be resolved
    raise RuntimeError(
        "get_direct_lake_source returned no rows. "
        "This typically means the model is not Direct Lake or has no Lakehouse/SQL endpoint binding."
    )

src = norm_cols(src_raw)
artifact_col = pick_col(src, ["artifact_type","artifacttype","source_type"])
artifact_type = str(src.iloc[0][artifact_col]).strip() if artifact_col else ""
print(f"Source artifact_type: {artifact_type or '(unknown)'}")

if artifact_type.lower() == "lakehouse":
    print("✅ The semantic model is LAKEHOUSE-sourced (Direct Lake helpers will apply).")
elif artifact_type.lower() == "warehouse":
    print("ℹ️ The semantic model is WAREHOUSE-sourced (Lakehouse-only helpers like schema_compare won’t apply).")
else:
    print("⚠️ Could not determine Lakehouse/Warehouse from metadata. Raw source:")
    display(src.head(10))




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


from sempy_labs import directlake
import re

DATASET   = "poc-aas-direct-lake-adb-v3"
WORKSPACE = "Integration-Darabricks"

# Run the compare
df_compare = directlake.direct_lake_schema_compare(
    dataset=DATASET,
    workspace=WORKSPACE
)

# Graceful handling of None or empty results
if df_compare is None:
    print("Compare returned None (no results).")
elif getattr(df_compare, "empty", False):
    print("Compare returned 0 rows — nothing to analyze (no tables or no differences).")
else:
    # Show first rows
    display(df_compare.head(20))

    # Identify columns that carry status/existence flags
    status_cols = [
        c for c in df_compare.columns
        if re.search(r"(status|exists|missing|mismatch)", c, flags=re.IGNORECASE)
    ]

    if not status_cols:
        print("No status/existence columns found to filter by.")
    else:
        # Convert to string and check for problematic values row-wise (vectorize-free)
        normalized = df_compare[status_cols].astype(str).apply(lambda s: s.str.lower())
        mask = normalized.apply(lambda row: row.isin(["false", "missing", "mismatch"]).any(), axis=1)

        problems = df_compare.loc[mask]
        if problems.empty:
            print("No issues detected in status/existence columns.")
        else:
            print(f"Found {len(problems)} potential issues:")
            display(problems)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Fabric notebook cell (Python)

from sempy_labs import directlake
import pandas as pd

DATASET_NAME   = "poc-aas-direct-lake-adb-v3"
WORKSPACE_NAME = "Integration-Darabricks"  # ensure exact spelling

def df_ok(df): return isinstance(df, pd.DataFrame) and not df.empty
def norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={c: c.strip().lower().replace(" ", "_") for c in df.columns})

# Query the source metadata
src = directlake.get_direct_lake_source(dataset=DATASET_NAME, workspace=WORKSPACE_NAME)

if not df_ok(src):
    raise RuntimeError(
        "get_direct_lake_source returned no rows. "
        "Common reasons: wrong names/IDs, model is not Direct Lake, or binding is missing."
    )

src_norm = norm_cols(src)
artifact_col = (
    "artifact_type"
    if "artifact_type" in src_norm.columns
    else next((c for c in src_norm.columns if "artifact" in c and "type" in c), None)
)

artifact_type = str(src_norm.iloc[0][artifact_col]).strip() if artifact_col else ""
print(f"Upstream artifact_type: {artifact_type}")

if artifact_type.lower() == "lakehouse":
    print("✅ LAKEHOUSE-sourced")
elif artifact_type.lower() == "warehouse":
    print("ℹ️ WAREHOUSE-sourced")
else:
    print("⚠️ Could not determine source type from returned columns.")
    display(src_norm.head(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Rebind all reports using the old semantic model to the new Direct Lake semantic model

# CELL ********************


from sempy_labs import directlake

# Your semantic model and workspace
DATASET_NAME   = "poc-aas-direct-lake-adb-v3"
WORKSPACE_NAME = "Integration-Darabricks"

# The Lakehouse you want to bind to
LAKEHOUSE_NAME = "migration_dq_tp_dl_v2"  # <-- replace with your Lakehouse name

# Rebind the semantic model to the Lakehouse
directlake.update_direct_lake_model_lakehouse_connection(
    dataset=DATASET_NAME,
    workspace=WORKSPACE_NAME,
    lakehouse=LAKEHOUSE_NAME,
    lakehouse_workspace=WORKSPACE_NAME  # same workspace
)

print("✅ Lakehouse connection updated.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rep.report_rebind_all(
    dataset=dataset_name,
    dataset_workspace=workspace_name,
    new_dataset=new_dataset_name,
    new_dataset_workspace=new_dataset_workspace_name,
    report_workspace=None
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Rebind reports one-by-one (optional)

# CELL ********************

report_name = 'poc-aas-direct-query-adb-dashboard' # Enter report name which you want to rebind to the new Direct Lake model

rep.report_rebind(
    report=report_name,
    dataset=new_dataset_name,
    report_workspace=workspace_name,
    dataset_workspace=new_dataset_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Show unsupported objects

# CELL ********************

dfT, dfC, dfR = directlake.show_unsupported_direct_lake_objects(dataset = dataset_name, workspace = workspace_name)

print('Calculated Tables are not supported...')
display(dfT)
print("Learn more about Direct Lake limitations here: https://learn.microsoft.com/power-bi/enterprise/directlake-overview#known-issues-and-limitations")
print('Calculated columns are not supported. Columns of binary data type are not supported.')
display(dfC)
print('Columns used for relationship must be of the same data type.')
display(dfR)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Schema check between semantic model tables/columns and lakehouse tables/columns
# 
# This will list any tables/columns which are in the new semantic model but do not exist in the lakehouse

# CELL ********************

directlake.direct_lake_schema_compare(dataset=new_dataset_name, workspace=new_dataset_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Show calculated tables which have been migrated to the Direct Lake semantic model as regular tables

# CELL ********************

directlake.list_direct_lake_model_calc_tables(dataset=new_dataset_name, workspace=new_dataset_workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Repoint tables (partitions) to different source tables

# CELL ********************

directlake.update_direct_lake_partition_entity(
    dataset=new_dataset_name,
    table_name=['Sales', 'Geography'], # Enter a list of table names to be repointed to the new source tables
    entity_name=['FactSales', 'DimGeography'], # Enter a list of the source tables (in the same order as table_name)
    schema=None,
    workspace=new_dataset_workspace_name,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Repoint the model to a different lakehouse/warehouse etc.

# CELL ********************

directlake.update_direct_lake_model_connection(
    dataset=new_dataset_name,
    workspace=new_dataset_workspace_name,
    source='MyLakehouse',
    source_type='Lakehouse', # 'Lakehouse' or 'Warehouse'
    source_workspace='MyLakehouseWorkspace',
    use_sql_endpoint=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Repoint tables within the model to a different lakehouse/warehouse etc.

# CELL ********************

directlake.update_direct_lake_model_connection(
    dataset=new_dataset_name,
    workspace=new_dataset_workspace_name,
    source='MyLakehouse',
    source_type='Lakehouse',
    source_workspace='MyLakehouseWorkspace',
    use_sql_endpoint=False,
    tables=['Sales', 'Budget']  # Specify the tables to update to the new source
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
