"""
Inventory Delivery Generator for GreenGrocer Analytics Pipeline

This module simulates realistic supply chain operations by generating
inventory delivery records for grocery stores. It creates delivery manifests
that occur on scheduled days (Monday and Thursday) with varying batch sizes
based on store type.

The generated data helps analyze:
- Inventory management efficiency
- Stock level optimization
- Supply chain performance
- Overstock/understock scenarios

Author: GreenGrocer Project
Date: 2025
"""

import json
import csv
import random
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
# Establish directory structure using script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)  # Go up to greengrocer_core

# Input: reference data (products and stores)
REFERENCE_DIR = os.path.join(PROJECT_ROOT, "data", "reference")
# Output: inventory delivery CSV files
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "inventory")

# Date range for generating inventory deliveries
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

# Delivery Schedule: 0=Monday, 3=Thursday
# Twice-weekly delivery is realistic for grocery retail
DELIVERY_DAYS = [0, 3]


def load_reference_data():
    """
    Load product and store master data from JSON files.
    
    Returns:
        tuple: A tuple containing:
            - products (list): List of product dictionaries
            - stores (list): List of store dictionaries
    
    Raises:
        FileNotFoundError: If reference data files are missing
        json.JSONDecodeError: If JSON files are malformed
    """
    p_path = os.path.join(REFERENCE_DIR, "products_master.json")
    s_path = os.path.join(REFERENCE_DIR, "stores_master.json")
    
    with open(p_path, 'r', encoding='utf-8') as f:
        products = json.load(f)
    with open(s_path, 'r', encoding='utf-8') as f:
        stores = json.load(f)
    
    return products, stores


def generate_deliveries(current_date, store, products):
    """
    Generate inventory delivery records for a single store on a delivery day.
    
    Simulates realistic supply chain behavior:
    - Store size determines batch size (Express stores get smaller batches)
    - Random product selection (not all products delivered every time)
    - Variable quantities (10-60 units) to create natural stock level variations
    
    This intentionally creates overstock/understock scenarios that mirror
    real-world supply chain challenges.
    
    Args:
        current_date (datetime): Date of the delivery
        store (dict): Store information dictionary containing:
            - store_id: Unique store identifier
            - typology: Store type (Express/Standard/Supercenter)
        products (list): List of all available products
    
    Returns:
        list: List of delivery record dictionaries with fields:
            - delivery_id: Unique delivery identifier
            - delivery_date: Date of delivery (YYYY-MM-DD)
            - store_id: Store receiving the delivery
            - product_id: Product being delivered
            - product_name: Product name (redundant but realistic)
            - quantity_delivered: Number of units delivered
            - delivery_status: Always "Received" (could be extended)
    """
    deliveries = []
    
    # Store size determines how MANY different products get restocked
    # Express stores: smaller batches (20-40 different products)
    # Standard/Supercenter: larger batches (50-90 different products)
    if store['typology'] == "Express":
        batch_size = random.randint(20, 40)
    else:
        batch_size = random.randint(50, 90)
    
    # Randomly select which products are being restocked today
    # This creates realistic scenarios where not everything is always in stock
    selected_products = random.sample(products, min(batch_size, len(products)))
    
    for product in selected_products:
        # Generate unique delivery ID using timestamp and random number
        delivery_id = f"DEL-{int(current_date.timestamp())}-{random.randint(10000, 99999)}"
        
        # QUANTITY LOGIC:
        # Deliver 10-60 units per product
        # Since typical sales are 5-30 units/week for popular items,
        # this creates natural overstock/understock scenarios for analysis
        qty = random.randint(10, 60)
        
        # Build delivery record
        row = {
            "delivery_id": delivery_id,
            "delivery_date": current_date.strftime("%Y-%m-%d"),
            "store_id": store['store_id'],
            "product_id": product['product_id'],
            "product_name": product['product_name'],  # Redundant but realistic for legacy systems
            "quantity_delivered": qty,
            "delivery_status": "Received"  # Could be extended to include "Pending", "Partial", etc.
        }
        deliveries.append(row)
    
    return deliveries


def main():
    """
    Main execution function for the inventory generator.
    
    Orchestrates the delivery data generation process:
    1. Loads reference data (products and stores)
    2. Creates output directory if needed
    3. Generates delivery records only on scheduled delivery days (Mon/Thu)
    4. Writes CSV files for each store on each delivery day
    
    Output: One CSV file per store per delivery day in OUTPUT_DIR
    Filename format: inventory_STORE_XXX_YYYYMMDD.csv
    """
    print("🚚 Starting Supply Chain Simulator (Inventory Generator)...")
    
    # Load master data
    products, stores = load_reference_data()
    
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"📁 Created output directory: {OUTPUT_DIR}")
    
    # Initialize counters
    current_date = START_DATE
    total_files = 0
    
    # Generate delivery data day by day
    while current_date <= END_DATE:
        # Only generate data on Mondays (0) and Thursdays (3)
        # weekday() returns 0=Monday, 1=Tuesday, ..., 6=Sunday
        if current_date.weekday() in DELIVERY_DAYS:
            
            # Progress indicator (print once per month on first Monday)
            if current_date.day <= 7 and current_date.weekday() == 0:
                print(f"   Restocking Month: {current_date.strftime('%Y-%m')}...", end='\r')
            
            # Generate deliveries for each store
            for store in stores:
                # Generate delivery batch for this store
                batch = generate_deliveries(current_date, store, products)
                
                # Create filename: inventory_STORE_001_20230102.csv
                filename = f"inventory_{store['store_id']}_{current_date.strftime('%Y%m%d')}.csv"
                filepath = os.path.join(OUTPUT_DIR, filename)
                
                # Define CSV column headers
                keys = [
                    "delivery_id",
                    "delivery_date",
                    "store_id",
                    "product_id",
                    "product_name",
                    "quantity_delivered",
                    "delivery_status"
                ]
                
                # Write CSV file
                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=keys)
                    writer.writeheader()
                    writer.writerows(batch)
                
                total_files += 1
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Final summary
    print(f"\n✅ Supply Chain Complete! Generated {total_files} Delivery Manifests.")
    print(f"📊 Date Range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print(f"📅 Delivery Schedule: {', '.join(['Monday' if d == 0 else 'Thursday' for d in DELIVERY_DAYS])}")
    print(f"🏪 Stores: {len(stores)}")
    print(f"📦 Products Available: {len(products)}")
    print(f"💾 Output Directory: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()