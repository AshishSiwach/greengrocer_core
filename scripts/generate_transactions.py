"""
Sales Data Chaos Generator for GreenGrocer Analytics Pipeline

This module generates realistic but intentionally messy sales transaction data
to simulate real-world data quality challenges. It introduces:
- Schema drift (changing column names over time)
- Product name typos and variations
- Duplicate transactions
- Seasonal purchasing patterns
- Weekly traffic variations

The generated chaos helps test data cleaning and ETL pipeline robustness.

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
# Output: raw sales CSV files
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data", "raw_sales")

# Date range for generating sales data
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2025, 12, 31)

# --- CHAOS SETTINGS ---
# Probability that a store will experience schema drift (column name change)
BAD_UPDATE_PROBABILITY = 0.30  # 30% of stores

# Probability that a product name will have a typo
TYPO_PROBABILITY = 0.15  # 15% of product names

# Probability of duplicate transaction
DUPLICATE_PROBABILITY = 0.03  # 3% of transactions

# Common product name variations and typos to inject
TYPO_MAPPINGS = {
    "Chocolat": ["Choco", "Choc.", "Cocolate", "Chocolat Noir"],
    "Lait": ["Lait UHT", "Lait.", "Mlk", "Lait Entier"],
    "Eau": ["Eau.", "Water", "H2O", "Eau Min."],
    "Sidi Ali": ["S. Ali", "Sidi-Ali", "SidiAli"],
    "Lindt": ["Lindt.", "LINDT", "Lidt"],
    "Coca": ["Coke", "Coca.", "Coca-Cola"],
    "Fromage": ["Fmg", "Fromage.", "Cheese"],
    "Pain": ["Pain.", "Bread", "Pain de Mie"],
    "Organic": ["Org.", "Bio", "Organic -"],
}


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


def assign_chaos_profiles(stores):
    """
    Assign schema drift dates to random stores (Rolling Update Scenario).
    
    Simulates a situation where 30% of stores receive a buggy software update
    that changes the date column name from "sale_date" to "date_of_sale".
    
    Args:
        stores (list): List of store dictionaries
    
    Returns:
        dict: Mapping of store_id to break_date (datetime or None)
            - If break_date is not None, schema drift occurs on that date
            - If None, the store maintains consistent schema
    """
    chaos_map = {}
    print("   🎲 Assigning Chaos Profiles (The Rolling Update Scenario):")
    
    for store in stores:
        store_id = store['store_id']
        
        # Randomly select stores for schema drift
        if random.random() < BAD_UPDATE_PROBABILITY:
            # Choose a random date within the generation period
            days_range = (END_DATE - START_DATE).days
            random_days = random.randint(100, days_range - 100)
            break_date = START_DATE + timedelta(days=random_days)
            
            chaos_map[store_id] = break_date
            print(f"      ⚠️  {store_id} will succumb to Schema Drift on {break_date.strftime('%Y-%m-%d')}")
        else:
            # Store maintains consistent schema
            chaos_map[store_id] = None
    
    return chaos_map


def messy_product_name(clean_name):
    """
    Introduce typos and variations into product names.
    
    Simulates data entry errors and inconsistent naming conventions
    that commonly occur in retail systems.
    
    Args:
        clean_name (str): Original product name
    
    Returns:
        str: Product name with possible typos/variations
    """
    # Only apply typos based on probability
    if random.random() > TYPO_PROBABILITY:
        return clean_name
    
    dirty_name = clean_name
    
    # Look for known words and replace with variations
    for word, variations in TYPO_MAPPINGS.items():
        if word.lower() in clean_name.lower():
            replacement = random.choice(variations)
            # Replace both capitalized and lowercase versions
            dirty_name = dirty_name.replace(word, replacement)
            dirty_name = dirty_name.replace(word.lower(), replacement)
    
    return dirty_name


def get_seasonal_weight(product, month):
    """
    Calculate seasonal purchase probability weight for a product.
    
    Simulates realistic seasonal purchasing patterns:
    - Summer (June-August): Increased beverage sales
    - Winter (November-December): Increased chocolate/snack sales
    
    Args:
        product (dict): Product dictionary with 'product_name' and 'category'
        month (int): Month number (1-12)
    
    Returns:
        float: Weight multiplier (1.0 = normal, >1.0 = more likely to sell)
    """
    weight = 1.0
    name = product['product_name'].lower()
    cat = product['category'].lower()
    
    # SUMMER (June, July, August) - Boost Drinks
    if month in [6, 7, 8]:
        if 'beverages' in cat or 'water' in name or 'juice' in name or 'coca' in name or 'sidi ali' in name:
            weight = 3.5  # 350% more likely to sell in summer
    
    # WINTER (November, December) - Boost Chocolate/Snacks
    if month in [11, 12]:
        if 'chocolate' in name or 'choc' in name or 'lindt' in name or 'biscuits' in name:
            weight = 2.0  # 200% more likely to sell in winter
    
    return weight


def generate_daily_sales(current_date, store, products, break_date):
    """
    Generate sales transactions for a single store on a single day.
    
    Incorporates multiple realistic patterns:
    - Higher traffic on weekends (Friday-Sunday)
    - Seasonal product preferences
    - Random product name typos
    - Occasional duplicate transactions
    - Schema drift after break_date (if applicable)
    
    Args:
        current_date (datetime): Date for which to generate sales
        store (dict): Store information dictionary
        products (list): List of available products
        break_date (datetime or None): Date when schema drift occurs
    
    Returns:
        list: List of transaction dictionaries with fields:
            - transaction_id: Unique transaction identifier
            - store_id: Store identifier
            - date_header: Column name for date ("sale_date" or "date_of_sale")
            - date_value: Transaction date
            - product_id: Product identifier
            - product_name: Product name (possibly with typos)
            - quantity: Number of items purchased
            - unit_price: Price per unit
            - total_amount: Total transaction amount
    """
    daily_transactions = []
    
    # 1. Volume Logic (Weekly Cycle)
    # Weekends (Friday=4, Saturday=5, Sunday=6) get 50% more traffic
    base_volume = 40 if store['typology'] == "Express" else 120
    if current_date.weekday() >= 4:  # Friday through Sunday
        base_volume = int(base_volume * 1.5)
    
    # Add randomness to daily volume
    num_txns = random.randint(base_volume - 20, base_volume + 40)
    
    # 2. Seasonality Logic
    # Calculate weights once per day for all products (performance optimization)
    current_month = current_date.month
    weights = [get_seasonal_weight(p, current_month) for p in products]
    
    # 3. Select products based on seasonal weights
    # Products with higher weights are more likely to be selected
    selected_products = random.choices(products, weights=weights, k=num_txns)
    
    # 4. Generate transactions
    for product in selected_products:
        # Create unique transaction ID using timestamp and random number
        txn_id = f"TXN-{int(current_date.timestamp())}-{random.randint(1000, 9999)}"
        
        # Apply potential typos to product name
        final_product_name = messy_product_name(product['product_name'])
        
        # Random quantity between 1 and 5 items
        quantity = random.randint(1, 5)
        
        # Build transaction record
        row = {
            "transaction_id": txn_id,
            "store_id": store['store_id'],
            "date_header": "sale_date",  # Original column name
            "date_value": current_date.strftime("%Y-%m-%d"),
            "product_id": product['product_id'],
            "product_name": final_product_name,
            "quantity": quantity,
            "unit_price": product['price_sell'],
            "total_amount": round(product['price_sell'] * quantity, 2)
        }
        
        # Schema Drift: Change date column name after break_date
        if break_date and current_date >= break_date:
            row['date_header'] = "date_of_sale"  # Buggy update changes column name
        
        daily_transactions.append(row)
        
        # Duplicate Injection: Randomly create duplicate transactions
        if random.random() < DUPLICATE_PROBABILITY:
            daily_transactions.append(row.copy())  # Exact duplicate
    
    return daily_transactions


def main():
    """
    Main execution function for the chaos generator.
    
    Orchestrates the entire sales data generation process:
    1. Loads reference data (products and stores)
    2. Assigns chaos profiles (schema drift dates) to stores
    3. Generates daily sales for each store across the date range
    4. Writes CSV files with intentional data quality issues
    
    Output: One CSV file per store per day in OUTPUT_DIR
    Filename format: sales_STORE_XXX_YYYYMMDD.csv
    """
    print("🚀 Starting Chaos Engine (Seasonal + Rolling Updates)...")
    
    # Load master data
    products, stores = load_reference_data()
    
    # Create output directory if it doesn't exist
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"📁 Created output directory: {OUTPUT_DIR}")
    
    # Assign which stores will experience schema drift and when
    chaos_map = assign_chaos_profiles(stores)
    
    # Initialize counters
    current_date = START_DATE
    total_files = 0
    
    # Generate sales data day by day
    while current_date <= END_DATE:
        # Progress indicator (print once per month)
        if current_date.day == 1:
            print(f"   Processing Month: {current_date.strftime('%Y-%m')}...", end='\r')
        
        # Generate sales for each store on this day
        for store in stores:
            # Get the schema drift date for this store (if any)
            store_break_date = chaos_map.get(store['store_id'])
            
            # Generate transactions
            sales = generate_daily_sales(current_date, store, products, store_break_date)
            
            # Skip if no sales generated
            if not sales:
                continue
            
            # Create filename: sales_STORE_001_20230101.csv
            filename = f"sales_{store['store_id']}_{current_date.strftime('%Y%m%d')}.csv"
            filepath = os.path.join(OUTPUT_DIR, filename)
            
            # Determine CSV column headers
            # Note: date column name varies based on schema drift
            keys = ["transaction_id", "store_id", "product_id", "product_name", 
                   "quantity", "unit_price", "total_amount"]
            date_col_name = sales[0]['date_header']  # "sale_date" or "date_of_sale"
            keys.insert(2, date_col_name)  # Insert date column in 3rd position
            
            # Write CSV file
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=keys)
                writer.writeheader()
                
                for row in sales:
                    # Convert internal row format to CSV format
                    csv_row = row.copy()
                    del csv_row['date_header']  # Remove metadata fields
                    del csv_row['date_value']
                    csv_row[date_col_name] = row['date_value']  # Map to correct column name
                    writer.writerow(csv_row)
            
            total_files += 1
        
        # Move to next day
        current_date += timedelta(days=1)
    
    # Final summary
    print(f"\n✅ Done! Generated {total_files} CSV files.")
    print(f"📊 Date Range: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
    print(f"🏪 Stores: {len(stores)}")
    print(f"📦 Products: {len(products)}")
    print(f"💾 Output Directory: {os.path.abspath(OUTPUT_DIR)}")


if __name__ == "__main__":
    main()