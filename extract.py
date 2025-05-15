import pandas as pd
from sqlalchemy import create_engine
from db_config import SOURCE_DB_URI, STAGING_DB_URI

# Tables you want to extract and their custom queries (adapt as needed)
TABLES = {
    "salesorderheader": "SELECT salesorderid, orderdate, customerid, totaldue FROM sales.salesorderheader",
    "salesorderdetail": "SELECT salesorderid, productid, orderqty, unitprice FROM sales.salesorderdetail",
    "customer": "SELECT customerid, personid, storeid, territoryid FROM sales.customer",
    "product": "SELECT productid, name, productnumber, standardcost, listprice FROM production.product",
    "person": "SELECT businessentityid, firstname, lastname FROM person.person"
}

def extract_to_staging():
    """
    Extract data from OLTP (AdventureWorks) to staging database
    """
    try:
        # Connect to source and staging PostgreSQL DBs
        source_engine = create_engine(SOURCE_DB_URI)
        staging_engine = create_engine(STAGING_DB_URI)

        for table_name, sql in TABLES.items():
            print(f"Extracting: {table_name}")
            df = pd.read_sql(sql, con=source_engine)
            
            # Load raw data into staging
            df.to_sql(table_name, con=staging_engine, if_exists='replace', index=False)
            print(f"✅ Loaded to staging: {table_name}")

    except Exception as e:
        print(f"❌ Extract failed: {e}")

if __name__ == "__main__":
    extract_to_staging()
