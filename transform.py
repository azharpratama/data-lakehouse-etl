import pandas as pd
from sqlalchemy import create_engine, text
from db_config import STAGING_DB_URI

def transform_staging_data():
    """
    Transform staging data to prepare for data warehouse loading
    Applies transformations to create star schema structure
    """
    try:
        # Connect to staging DB
        staging_engine = create_engine(STAGING_DB_URI)
        
        # Create dim_person first since it's referenced by other dimensions
        print("Transforming: dim_person")
        dim_person_query = """
            SELECT 
                businessentityid,
                firstname,
                lastname
            FROM person
        """
        dim_person_df = pd.read_sql(dim_person_query, con=staging_engine)
        dim_person_df.columns = [col.lower() for col in dim_person_df.columns]
        dim_person_df.to_sql('stg_dim_person', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_dim_person")

        # Transform fact_sales - with proper relationships to dimension tables
        print("Transforming: fact_sales")
        fact_sales_query = """
            SELECT 
                soh.salesorderid,
                TO_CHAR(soh.orderdate, 'YYYYMMDD') AS date_id,
                soh.orderdate,
                soh.customerid,
                sod.productid,
                sod.orderqty,
                sod.unitprice,
                sod.orderqty * sod.unitprice AS lineitemtotal,
                soh.totaldue
            FROM salesorderheader soh
            JOIN salesorderdetail sod ON soh.salesorderid = sod.salesorderid
        """
        fact_sales_df = pd.read_sql(fact_sales_query, con=staging_engine)
        
        # Ensure consistent column naming
        fact_sales_df.columns = [col.lower() for col in fact_sales_df.columns]
        
        # Write transformed fact table back to staging
        fact_sales_df.to_sql('stg_fact_sales', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_fact_sales")
        
        # Transform dim_customer
        print("Transforming: dim_customer")
        dim_customer_query = """
            SELECT 
                c.customerid,
                c.personid,
                c.storeid,
                c.territoryid,
                CASE WHEN p.businessentityid IS NOT NULL 
                     THEN CONCAT(p.firstname, ' ', p.lastname)
                     ELSE 'Unknown' 
                END AS customer_name
            FROM customer c
            LEFT JOIN person p ON c.personid = p.businessentityid
        """
        dim_customer_df = pd.read_sql(dim_customer_query, con=staging_engine)
        dim_customer_df.columns = [col.lower() for col in dim_customer_df.columns]
        dim_customer_df.to_sql('stg_dim_customer', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_dim_customer")
        
        # Transform dim_product
        print("Transforming: dim_product")
        dim_product_query = """
            SELECT 
                productid,
                name AS product_name, 
                productnumber,
                standardcost,
                listprice,
                (listprice - standardcost) AS margin
            FROM product
        """
        dim_product_df = pd.read_sql(dim_product_query, con=staging_engine)
        dim_product_df.columns = [col.lower() for col in dim_product_df.columns]
        dim_product_df.to_sql('stg_dim_product', con=staging_engine, if_exists='replace', index=False)
        print("✅ Transformed: stg_dim_product")
        
        # Add date dimension for better analytics
        print("Creating: dim_date")
        # Extract unique dates from salesorderheader
        date_query = "SELECT DISTINCT orderdate FROM salesorderheader"
        dates_df = pd.read_sql(date_query, con=staging_engine)
        
        # Create date dimension
        dim_date = []
        for date in dates_df['orderdate']:
            date_pd = pd.to_datetime(date)
            dim_date.append({
                'date_id': date_pd.strftime('%Y%m%d'),
                'full_date': date_pd,
                'year': date_pd.year,
                'quarter': (date_pd.month-1)//3 + 1,
                'month': date_pd.month,
                'month_name': date_pd.strftime('%B'),
                'day': date_pd.day,
                'day_of_week': date_pd.dayofweek,
                'day_name': date_pd.strftime('%A')
            })
        
        dim_date_df = pd.DataFrame(dim_date)
        dim_date_df.to_sql('stg_dim_date', con=staging_engine, if_exists='replace', index=False)
        print("✅ Created: stg_dim_date")
        
        # Ensure all fact table dimension keys are properly connected
        print("Validating dimension relationships")
        # Update fact_sales with date_id if needed
        fix_fact_date_query = """
            UPDATE stg_fact_sales 
            SET date_id = TO_CHAR(orderdate, 'YYYYMMDD') 
            WHERE date_id IS NULL
        """
        from sqlalchemy import text
        with staging_engine.connect() as conn:
            conn.execute(text(fix_fact_date_query))
            conn.commit()
        print("✅ Validated dimension relationships")
        
    except Exception as e:
        print(f"❌ Transform failed: {e}")

if __name__ == "__main__":
    transform_staging_data()