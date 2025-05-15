#!/bin/bash
# filepath: /Users/azhar/Developer/data-lakehouse-etl/cron_etl.sh
# Cron wrapper for ETL process

# Set path to your project directory - absolute path is required for cron
PROJECT_DIR="/Users/azhar/Developer/data-lakehouse-etl"

# Set date for logging
DATE=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE="$PROJECT_DIR/etl_log_$(date '+%Y%m%d').log"

# Activate conda environment if needed (uncomment and adjust as necessary)
# source ~/miniconda3/etc/profile.d/conda.sh
# conda activate exp

# Go to project directory
cd "$PROJECT_DIR" || {
    echo "[$DATE] Failed to change to project directory" >> "$LOG_FILE"
    exit 1
}

# Run the ETL pipeline and log output
echo "[$DATE] Starting ETL pipeline run" >> "$LOG_FILE"
./run.sh >> "$LOG_FILE" 2>&1
EXIT_CODE=$?

# Log completion status
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$DATE] ETL pipeline completed successfully" >> "$LOG_FILE"
else
    echo "[$DATE] ETL pipeline failed with exit code $EXIT_CODE" >> "$LOG_FILE"
fi

exit $EXIT_CODE
