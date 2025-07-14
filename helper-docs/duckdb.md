# DuckDB Quickstart: Inspecting Parquet Files

This guide covers how to use DuckDB (a fast, embeddable SQL OLAP database) to inspect Parquet files on the command line and in Python.

## 1. Using DuckDB from the Command Line

### Start the DuckDB CLI

```sh
duckdb
```

### Inspect a Parquet File

```sql
SELECT * FROM 'path/to/your_file.parquet' LIMIT 10;
```

### Show Column Names and Types

```sql
DESCRIBE SELECT * FROM 'path/to/your_file.parquet';
```

### Count Rows

```sql
SELECT COUNT(*) FROM 'path/to/your_file.parquet';
```

You can also query with WHERE clauses or aggregations directly:

```sql
SELECT created_date, COUNT(*) FROM 'path/to/your_file.parquet'
GROUP BY created_date ORDER BY created_date;
```

### Alternate Loading Approach

```sql
.open FILENAME
show tables
```

## 2. Using DuckDB in Python

### Install DuckDB

If you haven't already:

```sh
pip install duckdb
```

### Basic Usage in a Script

```python
import duckdb

# Replace with your file path
file_path = "path/to/your_file.parquet"

# Load into a DataFrame
df = duckdb.query(f"SELECT * FROM '{file_path}'").to_df()

# Inspect the data
print(df.head())
print(df.dtypes)
```

### Optional: Use Pandas or PyArrow Interop

DuckDB can return results as Pandas or Arrow tables:

```python
# Return as Arrow table
arrow_table = duckdb.query(f"SELECT * FROM '{file_path}'").arrow()
```

## 3. Tips

* DuckDB can read Parquet files directly *without importing* them into a database.
* Parquet files are columnar, so even big files can be queried quickly if you SELECT only a few columns.
* Use LIMIT clauses during exploration to reduce output and memory load.

## 4. Common Errors

* **File not found**: Make sure the file path is correct and quotes are used.
* **Permission denied**: Check file permissions if running as a different user.
* **Extension errors**: Parquet support is built-in, but some features (e.g., JSON or spatial queries) may require extensions.

---

This doc is safe to include in your repo as a quick reference for DuckDB + Parquet tooling.
