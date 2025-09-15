import requests
import json
from datetime import datetime
import os

# -----------------------------
# Configurations
# -----------------------------
OUTPUT_DIR = "/opt/airflow/data/raw/all_products"  # folder for storing raw JSON

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# -----------------------------
# Fetch All Products
# -----------------------------
def fetch_all_products():
    """
    Fetch all products from Fake Store API (ignoring category)
    """
    url = "https://fakestoreapi.com/products"
    try:
        response = requests.get(url)
        response.raise_for_status()  # raise error for bad status
        data = response.json()
        return data
    except requests.RequestException as e:
        print(f"Error fetching API: {e}")
        return []

# -----------------------------
# Save JSON Locally
# -----------------------------
def save_to_json(data):
    """
    Save the API response to a timestamped JSON file.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(OUTPUT_DIR, f"all_products_{timestamp}.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} products to {file_path}")
    return file_path

# -----------------------------
# Airflow Entrypoint
# -----------------------------
def fetch_api():
    """
    Entrypoint for Airflow DAG.
    Fetch products and save them locally.
    """
    products = fetch_all_products()
    if products:
        file_path = save_to_json(products)
        print(f"Fetch API task complete. Output: {file_path}")
        return file_path
    else:
        print("No data fetched.")
        return None

# -----------------------------
# Local Run (for testing)
# -----------------------------
if __name__ == "__main__":
    fetch_api()
