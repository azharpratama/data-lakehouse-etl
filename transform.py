import pandas as pd
from sqlalchemy import create_engine, text
from db_config import STAGING_DB_URI
from datetime import datetime

def transform_staging_data():
    """
    Transform staging data to prepare for data warehouse loading
    Applies transformations to create star schema structure according to data mapping
    """
    try:
        # Connect to staging DB
        staging_engine = create_engine(STAGING_DB_URI)
        
        # First create the DimDate dimension as it's needed by multiple fact tables
        print("Creating: stg_dim_date")
        
        # Extract unique dates from salesorderheader and employee tables
        orderdate_query = "SELECT DISTINCT orderdate FROM salesorderheader"
        hiredate_query = "SELECT DISTINCT hiredate FROM employee"
        startdate_query = "SELECT DISTINCT startdate FROM employeedepartmenthistory"
        
        # Combine all date sources
        dates_df_order = pd.read_sql(orderdate_query, con=staging_engine)
        dates_df_hire = pd.read_sql(hiredate_query, con=staging_engine)
        dates_df_start = pd.read_sql(startdate_query, con=staging_engine)
        
        # Combine all date columns into one series and convert to datetime to ensure proper comparison
        all_dates = pd.concat([
            pd.to_datetime(dates_df_order['orderdate']),
            pd.to_datetime(dates_df_hire['hiredate']),
            pd.to_datetime(dates_df_start['startdate'])
        ]).dropna().drop_duplicates()
        
        # Create date dimension
        dim_date = []
        for date in all_dates:
            date_pd = pd.to_datetime(date)
            date_id = int(date_pd.strftime('%Y%m%d'))  # Convert to integer
            
            dim_date.append({
                'date_id': date_id,
                'full_date': date_pd,
                'day': date_pd.day,
                'month': date_pd.month,
                'quarter': (date_pd.month-1)//3 + 1,
                'year': date_pd.year
            })
        
        dim_date_df = pd.DataFrame(dim_date)
        dim_date_df.to_sql('stg_dim_date', con=staging_engine, if_exists='replace', index=False)
        print("✅ Created: stg_dim_date")
        
        # Transform DimTerritory dimension table
        print("Transforming: stg_dim_territory")
        dim_territory_query = """
            SELECT 
                territoryid AS Territory_ID,
                name AS Territory_Name,
                "group" AS Region
            FROM salesterritory
        """
        dim_territory_df = pd.read_sql(dim_territory_query, con=staging_engine)
        dim_territory_df.to_sql('stg_dim_territory', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_dim_territory")
        
        # Transform DimDepartment dimension table
        print("Transforming: stg_dim_department")
        dim_department_query = """
            SELECT 
                CAST(departmentid AS INTEGER) AS Department_ID,
                name AS Department_Name
            FROM department
        """
        dim_department_df = pd.read_sql(dim_department_query, con=staging_engine)
        dim_department_df.to_sql('stg_dim_department', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_dim_department")
        
        # Transform DimEmployee dimension table
        print("Transforming: stg_dim_employee")
        dim_employee_query = """
            SELECT 
                e.employeeid AS Employee_ID,
                p.firstname AS First_Name,
                p.lastname AS Last_Name,
                e.gender AS Gender
            FROM employee e
            JOIN person p ON e.employeeid = p.businessentityid
        """
        dim_employee_df = pd.read_sql(dim_employee_query, con=staging_engine)
        dim_employee_df.to_sql('stg_dim_employee', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_dim_employee")
        
        # Transform FactSales fact table
        print("Transforming: stg_fact_sales")
        fact_sales_query = """
            SELECT 
                soh.salesorderid AS Sales_ID,
                soh.territoryid AS Territory_ID,
                CAST(TO_CHAR(soh.orderdate, 'YYYYMMDD') AS INTEGER) AS Date_ID,
                CAST(soh.totaldue AS DECIMAL(18,2)) AS Sale_Amount
            FROM salesorderheader soh
        """
        fact_sales_df = pd.read_sql(fact_sales_query, con=staging_engine)
        fact_sales_df.to_sql('stg_fact_sales', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_fact_sales")
        
        # Transform FactEmployeeTenure fact table 
        print("Transforming: stg_fact_employee_tenure")
        # Get the current date for tenure calculation
        current_date = datetime.now().date()
        
        # Create a SQL query that calculates tenure in years
        fact_employee_tenure_query = """
            SELECT 
                employeeid AS Employee_ID,
                hiredate AS Hire_Date,
                CAST('%s' AS DATE) AS Snapshot_Date,
                EXTRACT(YEAR FROM AGE(CAST('%s' AS DATE), hiredate)) AS Tenure_Years
            FROM employee
        """ % (current_date, current_date)
        
        fact_employee_tenure_df = pd.read_sql(fact_employee_tenure_query, con=staging_engine)
        fact_employee_tenure_df.to_sql('stg_fact_employee_tenure', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_fact_employee_tenure")
        
        # Transform FactHeadcount fact table
        print("Transforming: stg_fact_headcount")
        fact_headcount_query = """
            SELECT 
                CAST(departmentid AS INTEGER) AS Department_ID,
                CAST(TO_CHAR(startdate, 'YYYYMMDD') AS INTEGER) AS Date_ID,
                COUNT(employeeid) AS Employee_Count
            FROM employeedepartmenthistory
            GROUP BY departmentid, CAST(TO_CHAR(startdate, 'YYYYMMDD') AS INTEGER)
        """
        fact_headcount_df = pd.read_sql(fact_headcount_query, con=staging_engine)
        fact_headcount_df.to_sql('stg_fact_headcount', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_fact_headcount")
        
        print("✅ All transformations completed successfully")
        
    except Exception as e:
        print(f"❌ Transform failed: {e}")

if __name__ == "__main__":
    transform_staging_data()
