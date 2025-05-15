#!/usr/bin/env python3
# filepath: /Users/azhar/Developer/dlh-etl-adventureworks/run.py
"""
ETL Pipeline Runner Script

This script runs the complete ETL pipeline in sequence:
1. Extract data from source to staging
2. Transform data in staging
3. Load data from staging to data warehouse
"""

import time
import subprocess
import sys
from datetime import datetime

def run_script(script_name, description):
    """Run a Python script and return whether it was successful"""
    print(f"\n{'='*80}")
    print(f"🚀 {description} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}")
    
    result = subprocess.run([sys.executable, script_name], capture_output=False)
    
    if result.returncode == 0:
        print(f"\n✅ {description} completed successfully\n")
        return True
    else:
        print(f"\n❌ {description} failed with exit code {result.returncode}\n")
        return False

def main():
    """Run the full ETL pipeline"""
    start_time = time.time()
    print(f"\n🔄 Starting ETL pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Extract
    if not run_script('extract.py', 'Extract data from OLTP to staging'):
        print("❌ Pipeline stopped due to extraction failure")
        return False
    
    # Step 2: Transform
    if not run_script('transform.py', 'Transform data in staging'):
        print("❌ Pipeline stopped due to transformation failure")
        return False
    
    # Step 3: Load
    if not run_script('load.py', 'Load data from staging to warehouse'):
        print("❌ Pipeline stopped due to loading failure")
        return False
    
    # Pipeline completed successfully
    elapsed_time = time.time() - start_time
    print(f"\n🏁 ETL pipeline completed successfully in {elapsed_time:.2f} seconds")
    print(f"⏰ Finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return True

if __name__ == "__main__":
    main()
