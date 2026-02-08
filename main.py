import json
import os
import sys

# Ensure 'src' folder is visible to Python
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.loader import load_data
from src.processor import process_data
from src.visualizer import show_dashboard

# Constants
CONFIG_FILE = 'config.json'
# Path to data folder
DATA_FILE = os.path.join('data', 'gdp_dataset.csv')

def load_config():
    """Loads configuration settings from JSON file."""
    if not os.path.exists(CONFIG_FILE):
        raise FileNotFoundError("config.json file missing.")
    
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def main():
    print("---------------------------------------")
    print("   GDP Analysis System (SDA 2026)     ")
    print("---------------------------------------")
    
    try:
        # 1. Load Configuration
        print("Step 1: Loading Configuration...")
        config = load_config()
        print(f"   -> Region: {config['region']}")
        print(f"   -> Year: {config['year']}")
        
        # 2. Load Data
        print(f"\nStep 2: Loading Dataset from '{DATA_FILE}'...")
        if not os.path.exists(DATA_FILE):
             print(f"\n[ERROR] File not found: {DATA_FILE}")
             return

        df = load_data(DATA_FILE)
        
        # 3. Process Data
        print("\nStep 3: Processing Data...")
        processed_data, result_val = process_data(df, config)
        
        print(f"   -> Calculation ({config['operation']}): {result_val:,.2f}")
        
        # 4. Visualization
        if config['output'] == 'dashboard':
            print("\nStep 4: Launching Visualizations...")
            print("   (Graphs will open sequentially. Close one to see the next.)")
            show_dashboard(processed_data, result_val, config)
            
    except FileNotFoundError as fnf:
        print(f"\n[ERROR] File missing: {fnf}")
    except Exception as e:
        print(f"\n[ERROR] An error occurred: {e}")

if __name__ == "__main__":
    main()
