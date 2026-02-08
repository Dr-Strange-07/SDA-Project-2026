import pandas as pd
import os

# Paths
file_path = os.path.join('data', 'gdp_dataset.csv')
fixed_path = os.path.join('data', 'gdp_dataset_fixed.csv')

def check_and_fix():
    print(f"Checking file: {file_path}...")

    if not os.path.exists(file_path):
        print("Error: File not found!")
        return

    # 1. Check if it's hidden Excel file (starts with 'PK')
    try:
        with open(file_path, 'rb') as f:
            header = f.read(2)
        if header == b'PK':
            print("Detected Excel format. Converting to CSV...")
            df = pd.read_excel(file_path, engine='openpyxl')
            df.to_csv(fixed_path, index=False)
            print(f"Success! Converted to {fixed_path}")
            return
    except Exception:
        pass

    # 2. If not Excel, try reading as standard CSV
    print("File appears to be text/CSV. Verifying content...")
    try:
        # Try reading standard CSV
        df = pd.read_csv(file_path)
        print("Good News: The file is already a VALID CSV.")
        print(f"Columns detected: {list(df.columns[:3])}...")
        
        # Save clean copy anyway to ensure standard formatting
        df.to_csv(fixed_path, index=False)
        print(f"Verified copy saved to: {fixed_path}")

    except Exception as e:
        print(f"\nWarning: Standard read failed ({e}).")
        print("Attempting to repair CSV...")
        try:
            # Try auto-detecting separator and skipping bad lines
            df = pd.read_csv(file_path, sep=None, engine='python', on_bad_lines='skip')
            df.to_csv(fixed_path, index=False)
            print(f"Repaired CSV saved to: {fixed_path}")
        except Exception as e2:
            print(f"Repair failed: {e2}")
            print("\n--- First 5 lines of your file ---")
            with open(file_path, 'r', errors='ignore') as f:
                for i in range(5):
                    print(f.readline().strip())

if __name__ == "__main__":
    check_and_fix()
