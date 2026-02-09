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

# --- UPDATE: Ab hum FIXED wali file use kar rahe hain ---
DATA_FILE = os.path.join('data', 'gdp_dataset_fixed.csv') 

def load_config():