import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns

# 1. CONNEXION SQL SERVER
def connect_sql_server():
    try:
        conn_sql = pyodbc.connect(
            'DRIVER={SQL Server};'
            'SERVER=DESKTOP-PT9IUSD\\MYSERVERR;'  
            'DATABASE=Northwind;' #Northwind
            'Trusted_Connection=yes;'
        )
        print("Connexion SQL Server réussie")
        return conn_sql
    except Exception as e:
        print(f"Erreur SQL Server: {e}")
        return None
    
def connect_data_werehouse():
    try:
        conn_sql = pyodbc.connect(
            'DRIVER={SQL Server};'
            'SERVER=DESKTOP-PT9IUSD\\MYSERVERR;'  
            'DATABASE=NEWW;' #Northwind
            'Trusted_Connection=yes;'
        )
        print("Connexion SQL Server réussie")
        return conn_sql
    except Exception as e:
        print(f"Erreur SQL Server: {e}")
        return None

# 2. CHARGEMENT DES FICHIERS EXCEL (Access)
def load_excel_files():
    excel_files = {
        'orders': 'Orders.xlsx',
        'customers': 'Customers.xlsx', 
        'employees': 'Employees.xlsx',
        'order_details': 'Order Details.xlsx'
    }
    
    dataframes = {}
    for name, file in excel_files.items():
        try:
            dataframes[name] = pd.read_excel(file)
            print(f"Fichier {file} chargé")
        except Exception as e:
            print(f"Erreur chargement {file}: {e}")
    
    return dataframes



def get_access_connection():
    access_file_path = r"C:\\Users\\WINDOWS\\Desktop\\maybe\\data\\Northwind 2012.accdb"
    try:
        if access_file_path.endswith('.accdb'):
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + access_file_path + ';'
            )
        
        elif access_file_path.endswith('.mdb'):
            conn_str = (
                r'DRIVER={Microsoft Access Driver (*.mdb)};'
                r'DBQ=' + access_file_path + ';'
            )
        else:
            raise ValueError("Format de fichier non supporté. Utilisez .accdb ou .mdb")
        
        connection = pyodbc.connect(conn_str)
        print(f"Connexion réussie à : {access_file_path}")
        return connection
        
    except pyodbc.Error as e:
        print(f"Erreur de connexion Access: {e}")
        raise
    
def extract_table(source_type: str, table_name: str) -> pd.DataFrame:
    try:
        if source_type.lower() == 'sql':
            sql_engine = connect_sql_server()
            print(f"Extraction SQL: {table_name}")
            query = f"SELECT * FROM [{table_name}]"
            df = pd.read_sql(query, sql_engine)
            print(f"{len(df)} lignes extraites de {table_name} (SQL)")
            return df
            
        elif source_type.lower() == 'access':
            access_conn = get_access_connection()
            print(f"Extraction Access: {table_name}")
            query = f"SELECT * FROM [{table_name}]"
            df = pd.read_sql(query, access_conn)
            print(f"{len(df)} lignes extraites de {table_name} (Access)")
            return df
            
        else:
            raise ValueError("Type de source doit être 'sql' ou 'access'")
            
    except Exception as e:
        print(f"Erreur extraction {table_name} depuis {source_type}: {e}")
        raise



if __name__ == "__main__":
    conn = connect_sql_server()