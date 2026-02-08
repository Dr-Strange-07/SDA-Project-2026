import pandas as pd

def filter_by_region(df, region):
    """Filters dataframe by Continent (Region)."""
    if 'Continent' not in df.columns:
        raise KeyError("Column 'Continent' not found in dataset.")

    # Functional filtering
    filtered_df = df[df['Continent'] == region]
    
    if filtered_df.empty:
        raise ValueError(f"No data found for region: {region}")
    
    return filtered_df

def get_gdp_for_year(df, year):
    """Extracts specific year data and Country Names."""
    year_str = str(year)
    
    if year_str not in df.columns:
        raise ValueError(f"Year {year} not available in dataset.")
    
    selected_data = df[['Country Name', year_str]].copy()
    
    return selected_data.dropna()

def perform_operation(data, year, operation):
    """Performs sum or average operation."""
    year_str = str(year)
    values = data[year_str]
    
    if operation == "average":
        return values.mean()
    elif operation == "sum":
        return values.sum()
    else:
        raise ValueError(f"Unknown operation: {operation}")

def process_data(df, config):
    """Orchestrates the processing pipeline."""
    region = config.get('region')
    year = config.get('year')
    operation = config.get('operation')
    
    region_df = filter_by_region(df, region)
    year_data = get_gdp_for_year(region_df, year)
    result_value = perform_operation(year_data, year, operation)
    
    return year_data, result_value
