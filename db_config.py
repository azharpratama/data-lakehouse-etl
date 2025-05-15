"""
Database configuration module for ETL pipeline
Loads database connection settings from .env file
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Function to create database URI from environment variables
def create_db_uri(db_type):
    """Create SQLAlchemy database URI from environment variables"""
    host = os.getenv(f"{db_type}_DB_HOST")
    port = os.getenv(f"{db_type}_DB_PORT")
    name = os.getenv(f"{db_type}_DB_NAME")
    user = os.getenv(f"{db_type}_DB_USER")
    password = os.getenv(f"{db_type}_DB_PASSWORD")
    
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{name}"

# Database URIs
SOURCE_DB_URI = create_db_uri("OLTP")
STAGING_DB_URI = create_db_uri("STAGING")
DW_DB_URI = create_db_uri("DW")
