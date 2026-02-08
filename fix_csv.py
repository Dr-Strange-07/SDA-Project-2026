import pandas as pd
import os

# Aapki current file ka path (jo corrupt hai ya xlsx hai)
wrong_file = os.path.join('data', 'gdp_dataset.csv') 

# Nayi file ka path
correct_file = os.path.join('data', 'gdp_dataset_fixed.csv')

try:
    print("Trying to fix the file...")
    
    # Hum Excel engine se padhne ki koshish karenge bhale hi extension .csv ho
    df = pd.read_excel(wrong_file)
    
    # Ab sahi CSV format mein save karenge
    df.to_csv(correct_file, index=False)
    
    print(f"Success! Fixed file saved as: {correct_file}")
    print("Now update your main.py to use 'gdp_dataset_fixed.csv' OR rename this file.")
    
except Exception as e:
    print("Error:", e)
    print("Tip: Make sure the file in 'data' folder is actually an Excel file renamed to csv.")
