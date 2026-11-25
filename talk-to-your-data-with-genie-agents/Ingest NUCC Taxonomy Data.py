# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # NUCC Taxonomy → Ingest CSV into Delta Table
# MAGIC Reads `nucc_taxonomy_261.csv` from `/Volumes/<catalog>/provider/raw/nucc/` and loads `<catalog>.provider.taxonomy`.

# COMMAND ----------

# DBTITLE 1,Display Widgets
# Configuration widgets
catalogs = [row[0] for row in spark.sql('SHOW CATALOGS').collect()]
if not catalogs:
    raise RuntimeError('No accessible catalogs found.')

# Widgets match the NPPES ingestion notebook.
dbutils.widgets.dropdown('Catalog', catalogs[0], catalogs, '01. Target catalog')
dbutils.widgets.text('Schema', 'provider', '02. Target schema (auto-create)')
dbutils.widgets.text('Volume', 'raw', '03. Managed volume (auto-create)')
dbutils.widgets.text('CSV File', 'nucc_taxonomy_261.csv', '04. CSV filename in nucc folder')
dbutils.widgets.dropdown('Overwrite Tables', 'true', ['true', 'false'], '05. Replace existing Delta tables')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Retrieve Widget Selections and Set Variables
# MAGIC Run this section to retrieve widget selections and set variables used by subsequent cells.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Retrieve Parameters and Output
# Resolve and validate names and paths
import re

catalog = dbutils.widgets.get('Catalog').strip()
schema = dbutils.widgets.get('Schema').strip()
volume = dbutils.widgets.get('Volume').strip()
folder = 'nucc'
file_name = dbutils.widgets.get('CSV File').strip()
table_name = 'taxonomy'
overwrite = dbutils.widgets.get('Overwrite Tables').lower() == 'true'

for name, value in [('Catalog', catalog), ('Schema', schema), ('Volume', volume), ('Table Name', table_name)]:
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', value):
        raise ValueError(f'{name} must be a simple SQL identifier: {value!r}')
if not file_name.lower().endswith('.csv') or '/' in file_name or '\\' in file_name:
    raise ValueError('File Name must be a CSV filename without path separators.')
if folder and (folder.startswith('.') or '..' in folder.split('/')):
    raise ValueError('Folder must be a relative path inside the volume.')

volume_root = f'/Volumes/{catalog}/{schema}/{volume}'
source_path = f'{volume_root}/{folder}/{file_name}' if folder else f'{volume_root}/{file_name}'
target_table = f'`{catalog}`.`{schema}`.`{table_name}`'

print(f'Source: {source_path}')
print(f'Destination: {target_table}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Confirm File is Available
# MAGIC Will inspect the volume to ensure file is available to ingest.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Confirm File Exists
# Confirm that the file exists and can be read by this Serverless compute
parent = source_path.rsplit('/', 1)[0]

items = dbutils.fs.ls(parent)
matching = [item for item in items if item.name == file_name]

if not matching:
    raise FileNotFoundError(f'{file_name} not found in {parent}. Found: {[item.name for item in items]}')
print(f'Found source CSV ({matching[0].size:,} bytes)')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Source Data Frame with Friendly Column Names
# MAGIC Inspect the header row and generate column names for the table.

# COMMAND ----------

# DBTITLE 1,Read Column Header for Table Definition
# Read CSV. Preserve taxonomy codes and other fields as strings.
from pyspark.sql import functions as F

source_df = (spark.read.format('csv')
    .option('header', 'true')
    .option('inferSchema', 'false')
    .option('mode', 'FAILFAST')
    .option('multiLine', 'true')
    .option('quote', '"')
    .option('escape', '"')
    .load(source_path))

if not source_df.columns:
    raise ValueError('No CSV columns detected.')

# Make column names SQL-friendly while retaining all fields.
seen = set()
normalized_names = []
for index, original in enumerate(source_df.columns):
    cleaned = re.sub(r'[^A-Za-z0-9_]+', '_', original.strip()).strip('_')
    cleaned = cleaned or f'column_{index + 1}'
    if cleaned[0].isdigit():
        cleaned = f'col_{cleaned}'
    base = cleaned
    suffix = 2
    while cleaned.lower() in seen:
        cleaned = f'{base}_{suffix}'
        suffix += 1
    seen.add(cleaned.lower())
    normalized_names.append(cleaned)

source_df = source_df.toDF(*normalized_names)
source_count = source_df.count()

print(f'CSV rows: {source_count:,}')
print(f'Columns: {source_df.columns}')

if source_count == 0:
    raise ValueError('Source CSV has zero data rows. Refusing to overwrite a table.')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load Taxonomy Table
# MAGIC Ingests the CSV file into the final taxonomy Delta table.

# COMMAND ----------

# DBTITLE 1,Load Taxonomy Table
# Write and validate the Delta table
# The schema and volume are prepared in the setup cell.
result_df = (source_df
    .withColumn('_SourceFile', F.lit(file_name))
    .withColumn('_LoadedOn', F.current_timestamp()))

write_mode = 'overwrite' if overwrite else 'errorifexists'
(result_df.write.format('delta')
    .mode(write_mode)
    .option('overwriteSchema', 'true')
    .saveAsTable(target_table))

target_count = spark.table(target_table).count()
print(f'Wrote {target_count:,} rows to {target_table}')
if target_count != source_count:
    raise RuntimeError(f'Row-count mismatch: source={source_count:,}, target={target_count:,}')

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Inspect the Loaded Taxonomy Data

# COMMAND ----------

# DBTITLE 1,Confirm from Destination Table
display(spark.table(target_table).limit(20))