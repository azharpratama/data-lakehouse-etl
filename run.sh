#!/bin/bash
# filepath: /Users/azhar/Developer/dlh-etl-adventureworks/run.sh

# ETL Pipeline Runner Script (Bash version)
# This script runs the complete ETL pipeline in sequence

# Function to run a script and check its status
run_script() {
    script=$1
    description=$2
    
    echo -e "\n========================================================================="
    echo "🚀 $description - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "========================================================================="
    
    python3 "$script"
    status=$?
    
    if [ $status -eq 0 ]; then
        echo -e "\n✅ $description completed successfully\n"
        return 0
    else
        echo -e "\n❌ $description failed with exit code $status\n"
        return 1
    fi
}

# Start timing
START_TIME=$(date +%s)
echo -e "\n🔄 Starting ETL pipeline - $(date '+%Y-%m-%d %H:%M:%S')"

# Step 1: Extract
if ! run_script "extract.py" "Extract data from OLTP to staging"; then
    echo "❌ Pipeline stopped due to extraction failure"
    exit 1
fi

# Step 2: Transform
if ! run_script "transform.py" "Transform data in staging"; then
    echo "❌ Pipeline stopped due to transformation failure"
    exit 1
fi

# Step 3: Load
if ! run_script "load.py" "Load data from staging to warehouse"; then
    echo "❌ Pipeline stopped due to loading failure"
    exit 1
fi

# Calculate elapsed time
END_TIME=$(date +%s)
ELAPSED=$((END_TIME - START_TIME))

# Pipeline completed successfully
echo -e "\n🏁 ETL pipeline completed successfully in ${ELAPSED} seconds"
echo "⏰ Finished at: $(date '+%Y-%m-%d %H:%M:%S')"
