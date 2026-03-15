🎯 Goal
Load only new files, avoid duplicates, and standardize schema automatically.

📋 File Discovery Logic
```SQL
LIST @stage_path;
```

```JavaScript
if (file_name.startsWith('GET_VENDOR_SALES_REPORT')) {    file_list.push(file_name);}Show more lines
What this shows
```

* Dynamic file detection
* No hard‑coded filenames
* Scales as data volume grows


✅ Idempotent Load Protection
```SQL
SELECT DISTINCT Source_File FROM analytics_table;
```
```JavaScript
var new_files = file_list.filter(f => loaded_files.indexOf(f) === -1);
```

* Prevents duplicate ingestion
* Safe to rerun at any time
* Enterprise‑grade loading pattern


📥 Structured Load into Analytics Tables
```SQL
COPY INTO analytics_table
FROM (
  SELECT
    METADATA$FILENAME AS source_file,
    $1:"ASIN"::STRING,
    $1:"SHIPPEDREVENUE"::FLOAT
)
FILE_FORMAT = PARQUET;
```

* Schema enforcement
* Metadata capture for lineage
* Efficient columnar ingestion
