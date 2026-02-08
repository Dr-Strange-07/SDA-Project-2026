import matplotlib.pyplot as plt
import numpy as np

def show_dashboard(data, result_value, config):
    """
    Displays 4 sequential visualizations:
    Bar -> Histogram -> Dot Plot -> Pie Chart (with Stats)
    """
    region = config['region']
    year = str(config['year'])
    operation = config['operation']
    
    countries = data['Country Name'].tolist()
    gdp_values = data[year].tolist()
    
    # Sort data once for Bar and Dot plots
    combined = sorted(zip(gdp_values, countries))
    sorted_gdp, sorted_countries = zip(*combined)

    # 1. Bar Chart
    print("   -> Opening Graph 1/4: Bar Chart...")
    plt.figure(figsize=(12, 8))
    plt.barh(sorted_countries, sorted_gdp, color='teal')
    plt.title(f'1. GDP Comparison: {region} ({year})', fontsize=14)
    plt.xlabel('GDP (US$)')
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()

    # 2. Histogram
    print("   -> Opening Graph 2/4: Histogram...")
    plt.figure(figsize=(10, 6))
    plt.hist(gdp_values, bins=10, color='skyblue', edgecolor='black')
    plt.title(f'2. GDP Distribution Frequency', fontsize=14)
    plt.xlabel('GDP Range')
    plt.ylabel('Count of Countries')
    plt.grid(axis='y', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()

    # 3. Dot Plot
    print("   -> Opening Graph 3/4: Dot Plot...")
    plt.figure(figsize=(12, 8))
    plt.plot(sorted_gdp, sorted_countries, 'o', color='purple')
    plt.hlines(y=sorted_countries, xmin=0, xmax=sorted_gdp, color='gray', alpha=0.3)
    plt.title(f'3. GDP Values (Dot Plot)', fontsize=14)
    plt.xlabel('GDP (US$)')
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    plt.tight_layout()
    plt.show()

    # 4. Pie Chart + Stats
    print("   -> Opening Graph 4/4: Pie Chart & Final Report...")
    
    # Handle 'Others' for cleaner pie
    if len(gdp_values) > 8:
        sorted_indices = sorted(range(len(gdp_values)), key=lambda i: gdp_values[i], reverse=True)
        top_8 = sorted_indices[:8]
        top_countries = [countries[i] for i in top_8]
        top_values = [gdp_values[i] for i in top_8]
        top_countries.append('Others')
        top_values.append(sum(gdp_values) - sum(top_values))
    else:
        top_countries = countries
        top_values = gdp_values

    plt.figure(figsize=(10, 9))
    
    # FIXED: Using a safe colormap 'Paired' which has .colors attribute
    # If using newer matplotlib where .colors might differ, we use a manual list generator
    # Safe approach: generate colors from a colormap
    cmap = plt.get_cmap('Paired')
    colors = [cmap(i) for i in np.linspace(0, 1, len(top_values))]

    plt.pie(top_values, labels=top_countries, autopct='%1.1f%%', startangle=140, colors=colors)
    plt.title(f'4. GDP Market Share', fontsize=14)
    
    # Final Stats Box
    stats = (f"=== FINAL REPORT ===\n"
             f"REGION: {region}\nYEAR: {year}\n"
             f"OPERATION: {operation.upper()}\nRESULT: ${result_value:,.2f}")
    
    plt.figtext(0.5, 0.05, stats, ha="center", fontsize=12, family='monospace',
                bbox={"facecolor":"#fff8dc", "edgecolor":"orange", "boxstyle":"round,pad=1"})
    
    plt.tight_layout(rect=[0, 0.15, 1, 1])
    plt.show()
