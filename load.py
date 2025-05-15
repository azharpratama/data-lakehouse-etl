import pandas as pd
from sqlalchemy import create_engine, text
from db_config import STAGING_DB_URI, DW_DB_URI

# Tables to load from staging to data warehouse in order of dependencies
TABLES_TO_LOAD = [
    "stg_dim_person",
    "stg_dim_customer", 
    "stg_dim_product",
    "stg_dim_date",
    "stg_fact_sales"
]

# Target table names in data warehouse (removing 'stg_' prefix)
def get_target_name(staging_name):
    return staging_name.replace('stg_', '')

# SQL to create tables with proper primary and foreign keys
CREATE_TABLE_SQL = {
    "dim_person": """
        CREATE TABLE IF NOT EXISTS dim_person (
            businessentityid INTEGER PRIMARY KEY,
            firstname VARCHAR(50),
            lastname VARCHAR(50)
        )
    """,
    "dim_customer": """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customerid INTEGER PRIMARY KEY,
            personid INTEGER,
            storeid INTEGER, 
            territoryid INTEGER,
            customer_name VARCHAR(100),
            FOREIGN KEY (personid) REFERENCES dim_person(businessentityid)
        )
    """,
    "dim_product": """
        CREATE TABLE IF NOT EXISTS dim_product (
            productid INTEGER PRIMARY KEY,
            product_name VARCHAR(100),
            productnumber VARCHAR(25),
            standardcost NUMERIC(19,4),
            listprice NUMERIC(19,4),
            margin NUMERIC(19,4)
        )
    """,
    "dim_date": """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id VARCHAR(8) PRIMARY KEY,
            full_date DATE,
            year INTEGER,
            quarter INTEGER,
            month INTEGER,
            month_name VARCHAR(9),
            day INTEGER,
            day_of_week INTEGER,
            day_name VARCHAR(9)
        )
    """,
    "fact_sales": """
        CREATE TABLE IF NOT EXISTS fact_sales (
            salesorderid INTEGER,
            date_id VARCHAR(8),
            orderdate DATE,
            customerid INTEGER,
            productid INTEGER,
            orderqty INTEGER,
            unitprice NUMERIC(19,4),
            lineitemtotal NUMERIC(19,4),
            totaldue NUMERIC(19,4),
            PRIMARY KEY (salesorderid, productid),
            FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
            FOREIGN KEY (customerid) REFERENCES dim_customer(customerid),
            FOREIGN KEY (productid) REFERENCES dim_product(productid)
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
