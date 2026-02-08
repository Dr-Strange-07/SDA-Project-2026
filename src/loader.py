import pandas as pd
import os

def load_data(file_path):
    """
    Loads the GDP dataset from a CSV file.
    Follows Single Responsibility Principle (SRP).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' was not found.")

    try:
        df = pd.read_csv(file_path)
        
        # Basic Cleaning
        if 'Continent' in df.columns:
            df['Continent'] = df['Continent'].fillna('Unknown')
        
        # Functional: Strip whitespace from text columns
        text_cols = ['Country Name', 'Country Code', 'Continent']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
        
        print("   -> Data loaded successfully.")
        return df
        
    except Exception as e:
        raise ValueError(f"Error reading CSV file: {e}")
