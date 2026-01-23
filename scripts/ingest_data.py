"""
GreenGrocer Data Ingestion Pipeline

This script handles the ingestion of raw sales and inventory data from CSV files
into a PostgreSQL database. It's designed to handle schema drift and large volumes
of files efficiently using batch processing.

The script implements a Bronze Layer pattern where all data is initially stored
as strings, with type conversions and cleaning handled downstream in dbt.
"""

import os
import glob
import time
import polars as pl
from sqlalchemy import create_engine, text


# --- CONFIGURATION ---

# Database Connection String
# Format: postgresql://username:password@host:port/database
DB_CONNECTION = "postgresql://admin:adminpassword@localhost:5432/greengrocer"

# Directories
# We assume this script is in greengrocer_core/scripts/
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SALES_DIR = os.path.join(BASE_DIR, "data", "raw_sales")
INVENTORY_DIR = os.path.join(BASE_DIR, "data", "inventory")


# --- COLUMN MAPPING (Handling Schema Drift) ---

# Maps chaos variations to standard names
# This handles the common issue of inconsistent column naming across different data sources
SALES_SCHEMA_MAP = {
    "qty": "quantity",
    "quant": "quantity",
    "count": "quantity",
    "price": "unit_price",
    "cost": "unit_price",
    "tx_id": "transaction_id",
    "txn_id": "transaction_id",
    "prod_id": "product_id",
    "item_id": "product_id",
    "total": "total_amount",
    "amount": "total_amount",
    "date": "transaction_timestamp",
    "timestamp": "transaction_timestamp"
}


def get_db_engine():
    """
    Create and return a SQLAlchemy database engine.
    
    Returns:
        sqlalchemy.engine.Engine: Database connection engine
    """
    return create_engine(DB_CONNECTION)


def ingest_files(file_pattern, table_name, schema_map=None):
    """
    Ingest CSV files matching a pattern into a PostgreSQL table.
    
    This function processes files in batches to manage memory efficiently,
    handles schema drift through column mapping, and casts all data to strings
    for Bronze Layer storage.
    
    Args:
        file_pattern (str): Glob pattern to match CSV files (e.g., "data/*.csv")
        table_name (str): Name of the target database table
        schema_map (dict, optional): Dictionary mapping source column names to 
                                     standardized column names. Defaults to None.
    
    Process:
        1. Find all files matching the pattern
        2. Read each CSV with Polars (efficient for large files)
        3. Normalize column names using schema_map
        4. Cast all columns to strings (Bronze Layer pattern)
        5. Batch write to database to manage memory
    """
    # Find all matching files
    files = glob.glob(file_pattern)
    total_files = len(files)
    print(f"🚀 Found {total_files} files for table '{table_name}'...")
    
    # Get database connection
    engine = get_db_engine()
    
    # Process in chunks to save RAM
    batch_size = 500  # Number of files to process before writing to DB
    current_batch = []
    
    start_time = time.time()
    
    # Process each file
    for i, file_path in enumerate(files):
        try:
            # Read CSV lazily - Polars is faster than Pandas for large files
            df = pl.read_csv(file_path, ignore_errors=True)
            
            # Normalize column names to handle schema drift
            if schema_map:
                current_cols = df.columns
                rename_dict = {}
                
                # Build rename dictionary for columns that need mapping
                for col in current_cols:
                    if col in schema_map:
                        rename_dict[col] = schema_map[col]
                
                # Apply renaming if any mappings found
                if rename_dict:
                    df = df.rename(rename_dict)
            
            # Cast everything to String initially (Bronze Layer best practice)
            # We will fix data types later in dbt transformations
            df = df.select(pl.all().cast(pl.Utf8))
            current_batch.append(df)
            
            # Write batch to DB when batch size reached or on last file
            if len(current_batch) >= batch_size or i == total_files - 1:
                if current_batch:
                    # Combine all DataFrames in batch, handling mismatched schemas
                    combined_df = pl.concat(current_batch, how="diagonal")
                    
                    # Convert to Pandas and write to SQL
                    # (SQLAlchemy integration works best with Pandas)
                    combined_df.to_pandas().to_sql(
                        table_name, 
                        engine, 
                        if_exists='append',  # Append to existing table
                        index=False,         # Don't write DataFrame index
                        schema='public'      # Use public schema
                    )
                
                # Progress indicator (overwriting same line)
                print(f"   ✅ Processed {i+1}/{total_files} files...", end='\r')
                current_batch = []  # Clear batch after writing
                
        except Exception as e:
            # Log errors but continue processing other files
            print(f"\n❌ Error processing {file_path}: {e}")
    
    # Calculate and display total processing time
    duration = time.time() - start_time
    print(f"\n✨ Finished ingesting {table_name}. Time taken: {duration:.2f} seconds.")


def main():
    """
    Main execution function for the ingestion pipeline.
    
    Steps:
        1. Clear existing raw tables to start fresh
        2. Ingest sales data with schema mapping
        3. Ingest inventory data
        4. Report completion
    """
    print("🐘 Starting Ingestion Pipeline...")
    
    engine = get_db_engine()
    
    # Drop old tables to start fresh
    # This ensures we don't have duplicate or stale data
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS raw_sales;"))
        conn.execute(text("DROP TABLE IF EXISTS raw_inventory;"))
        conn.commit()
    print("🧹 Cleared old raw tables.")
    
    # Ingest Sales Data
    print("\n--- Processing SALES Data ---")
    sales_pattern = os.path.join(SALES_DIR, "*.csv")
    ingest_files(sales_pattern, "raw_sales", SALES_SCHEMA_MAP)
    
    # Ingest Inventory Data
    print("\n--- Processing INVENTORY Data ---")
    inventory_pattern = os.path.join(INVENTORY_DIR, "*.csv")
    ingest_files(inventory_pattern, "raw_inventory")
    
    print("\n🎉 All Data Successfully Loaded into Postgres!")


# Script entry point
if __name__ == "__main__":
    main()