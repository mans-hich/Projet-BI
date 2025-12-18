# config.py
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
from connect import connect_sql_server, connect_data_werehouse

class DatabaseConfig:
    # Configuration SQL Server Northwind (source)
    SQL_SERVER = pyodbc.connect(
        'DRIVER={SQL Server};'
        'SERVER=DESKTOP-PT9IUSD\\MYSERVERR;'  
        'DATABASE=Northwind;' #Northwind
        'Trusted_Connection=yes;'
    )
    
    # Configuration SQL Server Data Warehouse (destination)
    DW_SERVER = {
        'driver': '{SQL Server}',
        'server': 'DESKTOP-PT9IUSD\\MYSERVERR;',  # À adapter
        'database': 'NEWW',
        'trusted_connection': 'yes'
    }
    
    # Configuration Access (si nécessaire)
    ACCESS_DB_PATH = r'C:\\Users\\WINDOWS\\Desktop\\maybe\\data\\Northwind 2012.accdb'  # À adapter

def create_sql_connection():
    """Crée une connexion pyodbc à SQL Server"""
    return connect_sql_server()

def create_datawere_connection():
    return connect_data_werehouse()

def create_sqlalchemy_engine(config_dict):
    """Crée un engine SQLAlchemy"""
    conn_str = f"mssql+pyodbc://DESKTOP-PT9IUSD\\MYSERVERR/NEWW?" \
               f"driver=SQL+Server&trusted_connection=yes"
    return create_engine(conn_str) 

