"""
Reference Data Generator for GreenGrocer Analytics Pipeline

This module fetches real product data from OpenFoodFacts API and generates
synthetic store master data for the grocery analytics pipeline.

Author: GreenGrocer Project
Date: 2025
"""

import requests
import json
import random
import time
import os

# Configuration Constants
# Get the directory where this script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up one level to greengrocer_core, then into data/reference
OUTPUT_DIR = os.path.join(SCRIPT_DIR, '..', 'data', 'reference')
NUM_STORES = 20

# OpenFoodFacts API Endpoint
# We will search for products in specific categories to simulate a grocery store
CATEGORIES_TO_FETCH = [
    "snacks", "beverages", "dairies", "cereals", "meats", "cheeses", "breads"
]


def fetch_products_from_api():
    """
    Fetch real product data from OpenFoodFacts API.
    
    This function iterates through predefined grocery categories and retrieves
    product information including names, brands, and categories. It generates
    synthetic pricing and perishability data since the API doesn't provide this.
    
    Returns:
        list: A list of dictionaries, each containing product information:
            - product_id (str): Unique product identifier
            - product_name (str): Name of the product
            - brand (str): Brand name
            - category (str): Product category
            - price_sell (float): Retail selling price
            - price_cost (float): Cost price (40-70% of selling price)
            - is_perishable (bool): Whether the product is perishable
    """
    print("🌍 Connecting to OpenFoodFacts API...")
    products_master = []
    
    # Set user agent to identify our application
    headers = {"User-Agent": "GreenGrocerProject/1.0 (educational purpose)"}
    
    for category in CATEGORIES_TO_FETCH:
        print(f"   Fetching category: {category}...")
        
        # Construct API URL for specific category
        url = f"https://world.openfoodfacts.org/category/{category}.json"
        params = {
            "page_size": 20,  # Get top 20 items per category
            "fields": "code,product_name,categories_tags,brands"
        }
        
        try:
            response = requests.get(url, params=params, headers=headers, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                items = data.get('products', [])
                
                for item in items:
                    # Skip products without a name
                    if 'product_name' not in item or not item['product_name']:
                        continue
                    
                    # Generate realistic pricing (API doesn't provide prices)
                    # Random price between $2.00 and $15.00
                    sell_price = round(random.uniform(2.00, 15.00), 2)
                    
                    # Cost price is 40-70% of selling price (realistic markup)
                    cost_price = round(sell_price * random.uniform(0.4, 0.7), 2)
                    
                    # Determine if product is perishable based on category
                    is_perishable = category in ["dairies", "meats", "cheeses", "breads"]

                    products_master.append({
                        "product_id": item.get('code', f"GEN-{random.randint(10000, 99999)}"),
                        "product_name": item['product_name'],
                        "brand": item.get('brands', 'Unknown'),
                        "category": category,
                        "price_sell": sell_price,
                        "price_cost": cost_price,
                        "is_perishable": is_perishable
                    })
            else:
                print(f"   ❌ Failed to fetch {category}: Status {response.status_code}")
            
            # Be polite to the API - add delay between requests
            time.sleep(1)
            
        except Exception as e:
            print(f"   ⚠️ Error fetching {category}: {e}")

    print(f"✅ Fetched {len(products_master)} real products.")
    return products_master


def generate_stores():
    """
    Generate synthetic store master data.
    
    Creates store information for multiple locations across major US cities,
    with varying store sizes and typologies.
    
    Returns:
        list: A list of dictionaries, each containing store information:
            - store_id (str): Unique store identifier (e.g., "STORE_001")
            - city (str): City where store is located
            - typology (str): Store type (Express/Standard/Supercenter)
            - size_sqft (int): Store size in square feet
            - opened_date (str): Store opening date in YYYY-MM-DD format
    """
    print("🏢 Building Store Master Data...")
    stores = []
    
    # List of major US cities for store locations
    cities = [
        "New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
        "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose",
        "Austin", "Jacksonville", "Fort Worth", "Columbus", "Charlotte",
        "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington"
    ]
    
    for i in range(1, NUM_STORES + 1):
        city = cities[i - 1]
        store_id = f"STORE_{i:03d}"
        
        # Randomly assign store type and corresponding size
        size_type = random.choice(["Express", "Standard", "Supercenter"])
        
        if size_type == "Express":
            sq_ft = random.randint(2000, 5000)  # Small convenience stores
        elif size_type == "Standard":
            sq_ft = random.randint(10000, 30000)  # Regular grocery stores
        else:  # Supercenter
            sq_ft = random.randint(50000, 80000)  # Large format stores
        
        stores.append({
            "store_id": store_id,
            "city": city,
            "typology": size_type,
            "size_sqft": sq_ft,
            "opened_date": f"{random.randint(2010, 2022)}-{random.randint(1, 12):02d}-01"
        })
    
    return stores


def main():
    """
    Main execution function.
    
    Orchestrates the data generation process:
    1. Creates output directory if it doesn't exist
    2. Fetches real product data from OpenFoodFacts API
    3. Generates synthetic store master data
    4. Saves both datasets to JSON files in the reference folder
    """
    # Create output directory if it doesn't exist
    # Get absolute path for clarity
    abs_output_dir = os.path.abspath(OUTPUT_DIR)
    
    # exist_ok=True prevents errors if directory already exists
    os.makedirs(abs_output_dir, exist_ok=True)
    print(f"📁 Using directory: {abs_output_dir}")
    
    # Step 1: Fetch real products from API
    products = fetch_products_from_api()
    
    # Step 2: Generate synthetic store data
    stores = generate_stores()
    
    # Step 3: Save data to JSON files
    products_path = os.path.join(OUTPUT_DIR, "products_master.json")
    stores_path = os.path.join(OUTPUT_DIR, "stores_master.json")
    
    # Get absolute paths for display
    abs_products_path = os.path.abspath(products_path)
    abs_stores_path = os.path.abspath(stores_path)
    
    # Write products to JSON file
    with open(products_path, "w", encoding='utf-8') as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    print(f"✅ Products saved to: {abs_products_path}")
    
    # Write stores to JSON file
    with open(stores_path, "w", encoding='utf-8') as f:
        json.dump(stores, f, indent=2, ensure_ascii=False)
    print(f"✅ Stores saved to: {abs_stores_path}")
    
    print(f"\n🎉 Reference data generation complete!")
    print(f"📊 Generated {len(products)} products and {len(stores)} stores")


if __name__ == "__main__":
    main()