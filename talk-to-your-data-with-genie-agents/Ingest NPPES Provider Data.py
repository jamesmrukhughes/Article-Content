# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # CMS NPPES NPI → Ingest ZIP File into Delta Tables
# MAGIC Ingest an **already uploaded** CMS NPPES monthly full-replacement ZIP from a Unity Catalog volume.
# MAGIC
# MAGIC Select a catalog, choose a schema and managed volume (created if absent), and provide the **ZIP filename** already uploaded into that volume (or a relative path within the nppes folder). The notebook extracts CSVs into a timestamped folder within the volume and creates Delta tables.
# MAGIC
# MAGIC **Note:** The monthly dataset is a full replacement; the default overwrites Delta tables.

# COMMAND ----------

# DBTITLE 1,Display Widgets
import os, re, zipfile, shutil, json
from datetime import datetime, timezone
from pyspark.sql import functions as F

def ident(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError(f'Invalid Unity Catalog identifier: {value!r}')
    return f'`{value}`'

catalogs = [r[0] for r in spark.sql('SHOW CATALOGS').collect()]

if not catalogs:
    raise RuntimeError('No accessible catalogs. Check Unity Catalog permissions.')

# Widgets are created once; retain chosen values on notebook reruns.
dbutils.widgets.dropdown('Catalog', catalogs[0], catalogs, '01. Target catalog')
dbutils.widgets.text('Schema', 'provider', '02. Target schema (auto-create)')
dbutils.widgets.text('Volume', 'raw', '03. Managed volume (auto-create)')
dbutils.widgets.text('ZIP File', 'NPPES_Data_Dissemination_November_2026_V2.zip', '04. ZIP filename in nppes folder or relative path')
dbutils.widgets.dropdown('Overwrite Tables', 'true', ['true', 'false'], '05. Replace existing Delta tables')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Retrieve Widget Selections and Set Variables
# MAGIC Run this section to retrieve widget selections and set variables used by subsequent cells.

# COMMAND ----------

# DBTITLE 1,Retrieve Widget Selections and Set Variables
catalog = dbutils.widgets.get('Catalog').strip()
schema = dbutils.widgets.get('Schema').strip()
volume = dbutils.widgets.get('Volume').strip()
zip_relative = dbutils.widgets.get('ZIP File').strip()
overwrite = dbutils.widgets.get('Overwrite Tables').lower() == 'true'

for value in (catalog, schema, volume): ident(value)

volume_path = f'/Volumes/{catalog}/{schema}/{volume}'
nppes_path = f'{volume_path}/nppes'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Locate and Extract the ZIP already in the volume
# MAGIC Set the **ZIP File** widget to the uploaded filename (e.g., `NPPES_Data_Dissemination_November_2026_V2.zip`).

# COMMAND ----------

# DBTITLE 1,Extract ZIP Contents
if not zip_relative or not zip_relative.lower().endswith('.zip'):
    raise ValueError('ZIP File must be the name or relative path of a .zip archive')

if os.path.isabs(zip_relative) or '..' in zip_relative.replace('\\', '/').split('/'):
    raise ValueError('ZIP File must be a relative path inside the nppes folder of the selected volume')

source_zip = os.path.realpath(os.path.join(nppes_path, zip_relative))
volume_real = os.path.realpath(nppes_path)

if os.path.commonpath([source_zip, volume_real]) != volume_real:
    raise ValueError('ZIP File must be located inside the nppes folder of the selected volume')

if not os.path.isfile(source_zip):
    raise FileNotFoundError(f'ZIP not found: {source_zip}. Upload it to the volume and rerun from this cell.')

if not zipfile.is_zipfile(source_zip):
    raise ValueError(f'File is not a valid ZIP archive: {source_zip}')

run_id = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
raw_root = f'{nppes_path}/monthly/{run_id}'
os.makedirs(raw_root, exist_ok=True)
csv_paths = []
seen_names = set()

with zipfile.ZipFile(source_zip) as archive:
    for entry in archive.infolist():
        if entry.is_dir() or not entry.filename.lower().endswith('.csv'):
            continue
        
        filename = os.path.basename(entry.filename)
        
        if not filename:
            continue
        
        if filename.lower() in seen_names:
            raise ValueError(f'Duplicate CSV basename in archive: {filename}')

        seen_names.add(filename.lower())
        dest = os.path.join(raw_root, filename)
        print(f'Extracting {filename} ({entry.file_size:,} bytes)')
        
        with archive.open(entry) as source, open(dest, 'wb') as target:
            shutil.copyfileobj(source, target, length=8 * 1024 * 1024)
        
        csv_paths.append(dest)

if not csv_paths:
    raise RuntimeError('No CSV files found in uploaded ZIP')

print(f'Extracted {len(csv_paths)} CSV files to {raw_root}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ingest each CSV to a Delta table
# MAGIC All original fields are retained as strings to avoid losing leading zeros in identifiers, ZIP codes, and telephone numbers. CMS field headers are normalized to SQL-friendly snake_case; the original CSV remains in the volume. Provider core file becomes `providers`, with additional reference files mapped to descriptive table names.

# COMMAND ----------

# DBTITLE 1,Ingest Files into Delta Tables
#* Exclude CMS header-only files
csv_paths = [
    path for path in csv_paths
    if not os.path.basename(path).lower().endswith("_fileheader.csv")
]

print(f"Found {len(csv_paths)} data files for ingestion")

def get_table_name(filename):
    lower = filename.lower()
    if lower.startswith('npidata_pfile_'): return 'providers'
    if 'othername' in lower or 'other_name' in lower: return 'other_names'
    if 'pl_pfile' in lower or 'practice_location' in lower or 'practicelocation' in lower: return 'practice_locations'
    if 'endpoint' in lower: return 'endpoints'
    if 'otherpid' in lower or 'other_identifier' in lower: return 'other_identifiers'
    return re.sub(r'[^a-z0-9]+', '_', os.path.splitext(lower)[0]).strip('_')[:100]

def normalize_columns(df):
    seen = set()
    cols = []
    for col in df.columns:
        base = re.sub(r'[^a-z0-9]+', '_', col.strip().lower()).strip('_') or 'unnamed'
        name = base
        n = 2
        while name in seen:
            name = f'{base}_{n}'
            n += 1
        seen.add(name)
        cols.append(name)
    return df.toDF(*cols)

table_names = [get_table_name(os.path.basename(path)) for path in csv_paths]
if len(table_names) != len(set(table_names)):
    raise RuntimeError('Multiple source CSV files map to the same table')
if 'providers' not in table_names:
    raise RuntimeError('Main NPPES provider CSV is missing from the archive')

written = []
for path in csv_paths:
    filename = os.path.basename(path)
    table_name = get_table_name(filename)
    ident(table_name)
    full_name = f'{ident(catalog)}.{ident(schema)}.{ident(table_name)}'
    if not overwrite and spark.catalog.tableExists(f'{catalog}.{schema}.{table_name}'):
        raise RuntimeError(f'{full_name} exists and Overwrite Tables=false')

    print('Reading:', filename)
    df = (spark.read.format('csv')
          .option('header', 'true')
          .option('inferSchema', 'false')
          .option('multiLine', 'false')
          .option('mode', 'FAILFAST')
          .option('encoding', 'UTF-8')
          .load(path))
    df = normalize_columns(df)
    df = (df.withColumn('_source_file', F.lit(filename))
            .withColumn('_source_archive', F.lit(source_zip))
            .withColumn('_ingested_at', F.current_timestamp())
            .withColumn('_ingestion_run_id', F.lit(run_id)))
    if not df.take(1):
        raise RuntimeError(f'Refusing to overwrite a table with an empty CSV: {filename}')
    (df.write.format('delta').mode('overwrite' if overwrite else 'errorifexists')
       .option('overwriteSchema', 'true').saveAsTable(full_name))
    written.append({'table': f'{catalog}.{schema}.{table_name}', 'source_file': filename})
    print('Created:', full_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect the Loaded Provider Data

# COMMAND ----------

# DBTITLE 1,Execute Data Availability Query
display(spark.table(f"{ident(catalog)}.{ident(schema)}.providers").limit(20))