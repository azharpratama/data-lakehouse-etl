import pandas as pd
from sqlalchemy import create_engine
from db_config import SOURCE_DB_URI, STAGING_DB_URI

# Tables you want to extract and their custom queries (adapt as needed)
TABLES = {
    # Main tables for star schema
    "salesorderheader": "SELECT SalesOrderID, TerritoryID, OrderDate, TotalDue FROM sales.salesorderheader",
    "salesterritory": "SELECT TerritoryID, Name, CountryRegionCode, \"group\" FROM sales.salesterritory",
    
    # Employee-related tables
    "employee": "SELECT BusinessEntityID AS EmployeeID, NationalIDNumber, LoginID, Gender, HireDate FROM humanresources.employee",
    "person": "SELECT BusinessEntityID, FirstName, LastName FROM person.person",
    "department": "SELECT DepartmentID, Name, GroupName FROM humanresources.department",
    "employeedepartmenthistory": "SELECT BusinessEntityID AS EmployeeID, DepartmentID, StartDate, EndDate FROM humanresources.employeedepartmenthistory",
    
    # Keep any other tables needed for reference
    "salesorderdetail": "SELECT SalesOrderID, ProductID, OrderQty, UnitPrice FROM sales.salesorderdetail",
    "product": "SELECT ProductID, Name, ProductNumber, StandardCost, ListPrice FROM production.product"
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
