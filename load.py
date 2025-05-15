import pandas as pd
from sqlalchemy import create_engine, text
from db_config import STAGING_DB_URI, DW_DB_URI

# Tables to load from staging to data warehouse in order of dependencies
TABLES_TO_LOAD = [
    "stg_dim_date",
    "stg_dim_territory", 
    "stg_dim_department",
    "stg_dim_employee",
    "stg_fact_sales",
    "stg_fact_employee_tenure",
    "stg_fact_headcount"
]

# Target table names in data warehouse (removing 'stg_' prefix)
def get_target_name(staging_name):
    return staging_name.replace('stg_', '')

# SQL to create tables with proper primary and foreign keys
CREATE_TABLE_SQL = {
    "dim_date": """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date DATE,
            day INTEGER,
            month INTEGER,
            quarter INTEGER,
            year INTEGER
        )
    """,
    "dim_territory": """
        CREATE TABLE IF NOT EXISTS dim_territory (
            territory_id INTEGER PRIMARY KEY,
            territory_name VARCHAR(100),
            region VARCHAR(100)
        )
    """,
    "dim_department": """
        CREATE TABLE IF NOT EXISTS dim_department (
            department_id INTEGER PRIMARY KEY,
            department_name VARCHAR(100)
        )
    """,
    "dim_employee": """
        CREATE TABLE IF NOT EXISTS dim_employee (
            employee_id INTEGER PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            gender VARCHAR(10)
        )
    """,
    "fact_sales": """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sales_id INTEGER PRIMARY KEY,
            territory_id INTEGER,
            date_id INTEGER,
            sale_amount DECIMAL(18,2),
            FOREIGN KEY (territory_id) REFERENCES dim_territory(territory_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        )
    """,
    "fact_employee_tenure": """
        CREATE TABLE IF NOT EXISTS fact_employee_tenure (
            employee_id INTEGER,
            hire_date DATE,
            snapshot_date DATE,
            tenure_years INTEGER,
            PRIMARY KEY (employee_id),
            FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id)
        )
    """,
    "fact_headcount": """
        CREATE TABLE IF NOT EXISTS fact_headcount (
            department_id INTEGER,
            date_id INTEGER,
            employee_count INTEGER,
            PRIMARY KEY (department_id, date_id),
            FOREIGN KEY (department_id) REFERENCES dim_department(department_id),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id)
        )
    """
}

def load_to_dw():
    """
    Load transformed data from staging to data warehouse
    """
    try:
        # Connect to staging and DW
        staging_engine = create_engine(STAGING_DB_URI)
        dw_engine = create_engine(DW_DB_URI)
        
        # First create all tables with proper relationships
        for staging_table in TABLES_TO_LOAD:
            target_table = get_target_name(staging_table)
            
            # Create table with proper constraints if SQL is defined
            if target_table in CREATE_TABLE_SQL:
                print(f"Creating table schema: {target_table}")
                with dw_engine.connect() as connection:
                    connection.execute(text(CREATE_TABLE_SQL[target_table]))
                    connection.commit()
                print(f"✅ Created table schema: {target_table}")
        
        # Then load data into tables
        for staging_table in TABLES_TO_LOAD:
            target_table = get_target_name(staging_table)
            print(f"Loading: {staging_table} → {target_table}")
            
            # Truncate the target table first to avoid unique constraint violations
            truncate_query = f"TRUNCATE TABLE {target_table} CASCADE"
            with dw_engine.connect() as connection:
                connection.execute(text(truncate_query))
                connection.commit()
                print(f"- Truncated existing data from {target_table}")
            
            # Read from staging
            df = pd.read_sql(f"SELECT * FROM {staging_table}", con=staging_engine)
            
            # Load to DW, use correct method based on table exists status
            # Use if_exists='append' since tables are already created with proper schema
            df.to_sql(target_table, con=dw_engine, if_exists="append", index=False)
            print(f"✅ Loaded into DW: {target_table}")

    except Exception as e:
        print(f"❌ Load failed: {e}")

if __name__ == "__main__":
    load_to_dw()
