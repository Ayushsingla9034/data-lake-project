# Data Lake ETL Project

This project demonstrates a data pipeline for ingesting and transforming CSV data files into a SQLite database. It simulates how a data lake architecture ingests data on a daily basis.

## Data Files

There are three types of files stored in the raw data folder:

1. **CUST_MSTR_YYYYMMDD.csv**
   - Example: `CUST_MSTR_20191112.csv`
   - Requirements:
     - Add a column called `date` extracted from the filename in format `YYYY-MM-DD`
     - Load into the `CUST_MSTR` table in the database

2. **master_child_export-YYYYMMDD.csv**
   - Example: `master_child_export-20191112.csv`
   - Requirements:
     - Add two columns:  
       - `date` in format `YYYY-MM-DD`  
       - `date_key` in format `YYYYMMDD`
     - Load into the `MASTER_CHILD` table

3. **H_ECOM_ORDER.csv**
   - Load directly into the `H_ECOM_ORDER` table as is

The ETL script supports truncate-and-load to refresh data on a daily basis.

## ETL Process

The Python script (`daily_etl.py`) performs the following:
- Connects to the SQLite database
- Iterates through all CSV files in the date-specific folder
- Loads data into the correct tables as per rules above
- Adds required date columns by parsing the file names
- Uses `pandas` to process dataframes
- Uses `sqlite3` to insert the data

## Project Structure

```
data_lake_project/
  ├── raw_data/
  │   ├── 20231112/
  │   │   ├── CUST_MSTR_20231112.csv
  │   │   ├── master_child_export-20231112.csv
  │   │   └── H_ECOM_ORDER.csv
  ├── processed_data.db
  └── scripts/
      └── daily_etl.py
```

## How to Run

1. Place your CSVs inside the correct date folder under `raw_data/YYYYMMDD/`
2. Adjust the `date` variable in `daily_etl.py` if needed
3. Run the script:
```bash
python scripts/daily_etl.py
```
4. Check the SQLite database `processed_data.db` for results.

## Notes

- Data is loaded in truncate mode each day
- File names are critical to properly parse the dates
- Works with multiple files across multiple days

Feel free to fork and extend this project.
