# ==============================
# 1. Fetching Economic Data from FRED API
# ==============================

import requests
import pandas as pd
from fredapi import Fred
import os

# API Key for FRED
API_KEY = "b6059f63b93ffa5ea696dc1be23d075a"

# URL to fetch gasoline price data
url = f"https://api.stlouisfed.org/fred/series/observations?series_id=GASREGW&api_key={API_KEY}&file_type=json"

# Make API request
response = requests.get(url)

# Validate API response
if response.status_code == 200:
    print("✅ API key is working!")
    print(response.json())  # Display sample data
else:
    print("❌ API key is invalid or request failed.")

# Fetch additional economic data using fredapi
fred = Fred(api_key=API_KEY)

# Define date range
start_date = "2023-01-01"
end_date = "2023-12-31"

# Fetch Weekly Gas Prices and CPI Data
gas_prices = fred.get_series("GASREGW", start_date, end_date).reset_index()
gas_prices.columns = ["Week Start Date", "Avg Gas Price ($)"]

cpi = fred.get_series("CPIAUCSL", start_date, end_date).reset_index()
cpi.columns = ["Week Start Date", "CPI"]

# Merge the datasets
economic_data = pd.merge(gas_prices, cpi, on="Week Start Date", how="outer")

# Save economic data
output_dir = "../data/"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

economic_data.to_csv(output_dir + "economic_data.csv", index=False)
economic_data.to_csv(r"C:\Users\akank\Gen.AI\economic_data.csv", index=False)

print("✅ Economic data saved successfully!")

# ==============================
# 2. Generating Weekly Sales Data
# ==============================

import numpy as np
from datetime import datetime, timedelta

# Generate 50 products
products = [f"Product_{i}" for i in range(1, 51)]

# Generate weekly sales data for 52 weeks
start_date = datetime(2023, 1, 1)
weeks = [start_date + timedelta(weeks=i) for i in range(52)]

data = []

for week in weeks:
    for product in products:
        product_id = product.split("_")[1]
        units_sold = np.random.randint(50, 500)  # Random units sold
        price = np.random.uniform(5, 50)  # Price per unit
        discount = np.random.choice([0, 5, 10, 15])  # Discount in percentage
        revenue = units_sold * price * (1 - discount / 100)  # Apply discount
        data.append([week.strftime("%Y-%m-%d"), product_id, product, units_sold, price, discount, revenue, "USA"])

# Convert to DataFrame
sales_df = pd.DataFrame(data, columns=["Week Start Date", "Product ID", "Product Name", "Units Sold",
                                       "Price ($)", "Discount (%)", "Revenue ($)", "Region"])

# Save sales data
sales_df.to_csv(output_dir + "sales_data.csv", index=False)
sales_df.to_csv(r"C:\Users\akank\Gen.AI\sales_data.csv", index=False)

print("✅ Sales data generated and saved!")

# ==============================
# 3. Merging Sales and Economic Data
# ==============================

# Define file paths
sales_file = "../data/sales_data.csv"
economic_file = "../data/economic_data.csv"

if os.path.exists(sales_file) and os.path.exists(economic_file):
    # Load datasets
    sales_df = pd.read_csv(sales_file)
    economic_df = pd.read_csv(economic_file)

    # Merge on "Week Start Date"
    merged_df = pd.merge(sales_df, economic_df, on="Week Start Date", how="left")

    # Save merged data
    merged_df.to_csv(output_dir + "merged_data.csv", index=False)
    merged_df.to_csv(r"C:\Users\akank\Gen.AI\merged_data.csv", index=False)

    print("✅ Merged data saved successfully!")
else:
    print("❌ Error: One or both data files are missing!")

# ==============================
# 4. Data Integrity Checks
# ==============================

import logging

# Setup logging
log_dir = "../logs/"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(filename=os.path.join(log_dir, "alerts.log"), level=logging.WARNING, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

merged_file = "../data/merged_data.csv"

if os.path.exists(merged_file):
    df = pd.read_csv(merged_file)

    # Check for missing values
    if df.isnull().sum().sum() > 0:
        logging.warning("Missing values detected in merged data.")

    # Detect sales anomalies (spikes/drop > 50%)
    if "Product ID" in df.columns and "Revenue ($)" in df.columns:
        df["Revenue Change"] = df.groupby("Product ID")["Revenue ($)"].pct_change()
        anomalies = df[df["Revenue Change"].abs() > 0.5]

        if not anomalies.empty:
            logging.warning(f"Unexpected sales spikes/drops detected:\n{anomalies[['Week Start Date', 'Product ID', 'Revenue Change']]}")

    # Ensure all 52 weeks exist
    if len(df["Week Start Date"].unique()) < 52:
        logging.warning("Week Start Dates missing in merged dataset.")

    print("✅ Data integrity checks completed! Check '../logs/alerts.log' for warnings.")
else:
    print("❌ Error: Merged data file not found!")

# ==============================
# 5. Automating the Data Pipeline
# ==============================

import schedule
import time
import subprocess

# Function to run pipeline
def run_pipeline():
    print("🔄 Starting data pipeline...")
    logging.info("Starting data pipeline...")

    scripts = [
        "scripts/generate_sales.py",
        "scripts/fetch_fred_data.py",
        "scripts/merge_data.py",
        "scripts/data_checks.py"
    ]

    for script in scripts:
        try:
            subprocess.run(["python", script], check=True)
            logging.info(f"✅ {script} executed successfully.")
        except subprocess.CalledProcessError as e:
            logging.error(f"❌ Error executing {script}: {e}")
            print(f"Error executing {script}: {e}")

    print("✅ Data pipeline completed!")

# Schedule pipeline to run every 30 days
schedule.every(30).days.at("00:00").do(run_pipeline)

print("⏳ Scheduler started. Waiting for the next scheduled run...")

while True:
    schedule.run_pending()
    time.sleep(60)  # Check every 60 seconds
