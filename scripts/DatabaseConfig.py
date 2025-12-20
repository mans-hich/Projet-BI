#databaseconfig.py
import pyodbc
import pandas as pd
from sqlalchemy import create_engine, text
import warnings

warnings.filterwarnings("ignore")


class DatabaseConfig:
    SQL_SERVER_INSTANCE = "DIDI\\SQLEXPRESS"
    SOURCE_DATABASE = "Northwind"
    TARGET_DATABASE = "Dw"
    ACCESS_PATH = r"C:/Users/hicha/SPACE/9RAYA/bizb/Projet-BI/data/access/Nw.accdb"



def build_connection(db_name):
    conn_str = (
        "DRIVER={SQL Server};"
        f"SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};"
        f"DATABASE={db_name};"
        "Trusted_Connection=yes;"
    )
    return conn_str


def connect_to_database(db_name):
    try:
        connection = pyodbc.connect(build_connection(db_name))
        print(f"✅ Successfully connected to [{db_name}]")
        return connection
    except Exception as err:
        print(f"❌ Connection error on [{db_name}]: {err}")
        return None


def validate_connections():
    print("Testing source database connection...")
    source_conn = connect_to_database(DatabaseConfig.SOURCE_DATABASE)
    if source_conn:
        source_conn.close()
        print("Source database: PASSED")

    print("\nTesting data warehouse connection...")
    warehouse_conn = connect_to_database(DatabaseConfig.TARGET_DATABASE)
    if warehouse_conn:
        warehouse_conn.close()
        print("Data warehouse: PASSED")


if __name__ == "__main__":
    validate_connections()
