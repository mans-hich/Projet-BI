# etl_main.py - VERSION COMPLÈTE CORRIGÉE
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pyodbc
from sqlalchemy import create_engine, text
from DatabaseConfig import DatabaseConfig, create_sql_connection, create_datawere_connection
import create_database

class Northwind:
    def __init__(self):
        print("="*50)
        print("INITIALISATION ETL NORTHWIND")
        print("="*50)
        
        # Connexion à la source Northwind SQL
        print("\n1. Connexion à la source Northwind...")
        self.source_conn = create_sql_connection()
        
        if self.source_conn is None:
            raise Exception("❌ ÉCHEC: Impossible de se connecter à la base source Northwind")
        print("   ✅ Connexion source établie")
        
        # Vérifier/créer la base DW d'abord
        print("\n2. Vérification/création du data warehouse...")
        try:
            if create_database.create_datawarehouse():
                print("   ✅ Base DW vérifiée/créée")
                if create_database.create_dw_schema():
                    print("   ✅ Schéma DW vérifié/créé")
                else:
                    print("   ⚠️  Schéma DW non créé (peut-être déjà existant)")
            else:
                print("   ⚠️  Base DW non créée (peut-être déjà existante)")
        except Exception as e:
            print(f"   ⚠️  Erreur création DW: {e}")
    
        # Connexion au data warehouse
        print("\n3. Connexion au data warehouse...")
        self.dw_conn = create_datawere_connection()
        
        if self.dw_conn is None:
            # Essayer une connexion directe comme alternative
            print("   Tentative de connexion alternative...")
            try:
                self.dw_conn = pyodbc.connect(
                    'DRIVER={SQL Server};'
                    'SERVER=DESKTOP-PT9IUSD\\MYSERVERR;'  
                    'DATABASE=NEWW;'
                    'Trusted_Connection=yes;'
                )
                print("   ✅ Connexion DW établie (méthode alternative)")
            except Exception as e:
                print(f"   ❌ Impossible de se connecter au DW: {e}")
                raise Exception("❌ Impossible de se connecter au data warehouse")
        else:
            print("   ✅ Connexion DW établie")
        
        # Créer l'engine SQLAlchemy pour pandas to_sql (OPTIONNEL)
        print("\n4. Initialisation SQLAlchemy (optionnel)...")
        self.dw_engine = None  # On désactive SQLAlchemy pour éviter les erreurs
        print("   ℹ️  Utilisation pyodbc direct uniquement")
        
        print("\n" + "="*50)
        print("✅ Toutes les connexions sont établies")
        print("="*50)
    
    def check_table_exists(self, table_name):
        """Vérifie si une table existe dans le DW"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table_name}' 
                AND TABLE_CATALOG = '{DatabaseConfig.DW_SERVER['database']}'
            """)
            exists = cursor.fetchone()[0] > 0
            cursor.close()
            return exists
        except Exception as e:
            print(f"⚠️  Erreur vérification table {table_name}: {e}")
            return False
    
    
    def create_dim_date(self, start_year=1990, end_year=2025):
        print("\nDIMENSION DATE")
        print("-" * 30)
    
        # Vérifier si déjà remplie
        try:
            count = self.dw_conn.execute("SELECT COUNT(*) FROM DimDate").fetchone()[0]
            if count > 0:
                print(f" DimDate contient déjà {count:,} dates")
                return pd.DataFrame()
        except:
            pass  # table n'existe pas ou inaccessible → on continue
    
        print(f" Création des dates de {start_year} à {end_year}...")
    
        dates = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        dim_date = pd.DataFrame({
            'DateKey': dates.strftime('%Y%m%d').astype(int),
            'Date': dates.date,                    # ← directement des datetime.date
            'Year': dates.year,
            'Quarter': dates.quarter,
            'Month': dates.month,
            'Day': dates.day,
            'MonthName': dates.strftime('%B'),
            'DayOfWeek': dates.strftime('%A'),
            'IsWeekend': (dates.weekday >= 5).astype(int)
        })
    
        print(f" Chargement de {len(dim_date):,} dates dans SQL Server...")
    
        cursor = self.dw_conn.cursor()
        cursor.fast_executemany = True  # Magique
    
        # Préparation des données sous forme de liste de tuples
        data_to_insert = [
            (
                row.DateKey,
                row.Date,              # datetime.date → parfaitement géré avec Driver 17/18
                row.Year,
                row.Quarter,
                row.Month,
                row.Day,
                row.MonthName,
                row.DayOfWeek,
                row.IsWeekend
            )
            for row in dim_date.itertuples()
        ]
    
        cursor.executemany("""
            INSERT INTO DimDate 
            (DateKey, Date, Year, Quarter, Month, Day, MonthName, DayOfWeek, IsWeekend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data_to_insert)
    
        self.dw_conn.commit()
        cursor.close()
    
        print(f" DimDate créée avec succès : {len(dim_date):,} dates insérées")
        return dim_date
    
    def _create_dim_date_table(self):
        """Crée la table DimDate si elle n'existe pas"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimDate' AND xtype='U')
                CREATE TABLE DimDate (
                    DateKey INT PRIMARY KEY,
                    Date DATE NOT NULL,
                    Year INT NOT NULL,
                    Quarter INT NOT NULL,
                    Month INT NOT NULL,
                    Day INT NOT NULL,
                    MonthName VARCHAR(20),
                    DayOfWeek VARCHAR(20),
                    IsWeekend BIT,
                    UNIQUE(Date)
                )
            """)
            self.dw_conn.commit()
            cursor.close()
            print("  ✅ Table DimDate créée")
        except Exception as e:
            print(f"  ❌ Erreur création table DimDate: {e}")
                
    
    def extract_from_sql_server(self):
        """Extraction depuis SQL Server Northwind"""
        print("\n📥 EXTRACTION DONNÉES SOURCE")
        print("-"*30)
        
        queries = {
            'customers': """
                SELECT CustomerID, CompanyName, ContactName, ContactTitle, 
                       Address, City, Region, PostalCode, Country, Phone
                FROM Customers
                WHERE CustomerID IS NOT NULL
            """,
            'employees': """
                SELECT EmployeeID, LastName, FirstName, Title, TitleOfCourtesy,
                       BirthDate, HireDate, Address, City, Region, PostalCode,
                       Country, HomePhone, ReportsTo
                FROM Employees
                WHERE EmployeeID IS NOT NULL
            """,
            'orders': """
                SELECT o.OrderID, o.CustomerID, o.EmployeeID, 
                       o.OrderDate, o.RequiredDate, o.ShippedDate,
                       o.ShipVia, o.Freight, o.ShipName, o.ShipAddress,
                       o.ShipCity, o.ShipRegion, o.ShipPostalCode, o.ShipCountry,
                       SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) as TotalAmount
                FROM Orders o
                LEFT JOIN [Order Details] od ON o.OrderID = od.OrderID
                WHERE o.OrderID IS NOT NULL
                GROUP BY o.OrderID, o.CustomerID, o.EmployeeID, o.OrderDate, 
                         o.RequiredDate, o.ShippedDate, o.ShipVia, o.Freight,
                         o.ShipName, o.ShipAddress, o.ShipCity, o.ShipRegion,
                         o.ShipPostalCode, o.ShipCountry
                ORDER BY o.OrderID
            """
        }
        
        data = {}
        for name, query in queries.items():
            try:
                data[name] = pd.read_sql(query, self.source_conn)
                print(f"  ✅ {name}: {len(data[name])} lignes")
            except Exception as e:
                print(f"  ❌ Erreur extraction {name}: {e}")
                data[name] = pd.DataFrame()
        
        return data
    
    
    def extract_from_access(self):
        """Extraction depuis Access - ADAPTÉ À LA VRAIE STRUCTURE NORTHWIND"""
        if not DatabaseConfig.ACCESS_DB_PATH:
            print("\nℹ️  Pas de base Access configurée")
            return {}
    
        print("\n📥 EXTRACTION ACCESS (optionnel)")
        print("-"*30)
        
        try:
            access_conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            access_conn = pyodbc.connect(access_conn_str)
            
            # D'abord, voir quelles tables existent
            cursor = access_conn.cursor()
            tables = cursor.tables(tableType='TABLE')
            table_list = []
            for table in tables:
                table_list.append(table.table_name)
            cursor.close()
            
            print(f"  Tables disponibles dans Access: {table_list}")
            
            # Maintenant, utiliser les vraies tables Northwind
            access_data = {}
            
            # 1. Extraction des Clients (Customers)
            try:
                print("  Extraction des clients...")
                # Vérifier le nom exact de la table
                customer_table = 'Customers'
                if customer_table not in table_list:
                    # Essayer d'autres noms possibles
                    for table in table_list:
                        if 'customer' in table.lower():
                            customer_table = table
                            break
                
                query = f"SELECT * FROM [{customer_table}] WHERE [ID] IS NOT NULL"
                customers_df = pd.read_sql(query, access_conn)
                print(f"  ✅ Table {customer_table}: {len(customers_df)} lignes")
                
                # Afficher les colonnes pour vérification
                print(f"    Colonnes: {list(customers_df.columns)}")
                
                # Transformer selon la structure réelle
                # Dans votre fichier, les colonnes sont: ID, Company, Last Name, First Name, etc.
                customers_df = customers_df.rename(columns={
                    'ID': 'CustomerID',
                    'Company': 'CompanyName',
                    'Last Name': 'ContactLastName',
                    'First Name': 'ContactFirstName',
                    'Address': 'Address',
                    'City': 'City',
                    'State/Province': 'Region',
                    'ZIP/Postal Code': 'PostalCode',
                    'Country/Region': 'Country',
                    'Business Phone': 'Phone'
                })
                
                # Créer ContactName en combinant First Name et Last Name
                if 'ContactFirstName' in customers_df.columns and 'ContactLastName' in customers_df.columns:
                    customers_df['ContactName'] = customers_df['ContactFirstName'] + ' ' + customers_df['ContactLastName']
                
                # Garder seulement les colonnes nécessaires pour DimCustomer
                required_cols = ['CustomerID', 'CompanyName', 'ContactName', 'Address', 
                               'City', 'Region', 'PostalCode', 'Country', 'Phone']
                available_cols = [col for col in required_cols if col in customers_df.columns]
                
                if available_cols:
                    customers_df = customers_df[available_cols]
                    # Ajouter les colonnes manquantes
                    for col in required_cols:
                        if col not in customers_df.columns:
                            customers_df[col] = None
                    
                    customers_df['ContactTitle'] = 'Unknown'  # Pas dans la table Access
                    
                    access_data['customers_access'] = customers_df
                    print(f"  ✅ Clients Access transformés: {len(customers_df)} lignes")
                else:
                    print("  ❌ Colonnes requises non trouvées dans Customers")
                    access_data['customers_access'] = pd.DataFrame()
                    
            except Exception as e:
                print(f"  ❌ Erreur extraction clients Access: {e}")
                access_data['customers_access'] = pd.DataFrame()
            
            # 2. Extraction des Employés (Employees)
            try:
                print("  Extraction des employés...")
                employee_table = 'Employees'
                if employee_table not in table_list:
                    for table in table_list:
                        if 'employee' in table.lower():
                            employee_table = table
                            break
                
                query = f"SELECT * FROM [{employee_table}] WHERE [ID] IS NOT NULL"
                employees_df = pd.read_sql(query, access_conn)
                print(f"  ✅ Table {employee_table}: {len(employees_df)} lignes")
                
                # Transformer selon la structure réelle
                employees_df = employees_df.rename(columns={
                    'ID': 'EmployeeID',
                    'Last Name': 'LastName',
                    'First Name': 'FirstName',
                    'Job Title': 'Title',
                    'Business Phone': 'HomePhone',  # À adapter selon votre besoin
                    'Address': 'Address',
                    'City': 'City',
                    'State/Province': 'Region',
                    'ZIP/Postal Code': 'PostalCode',
                    'Country/Region': 'Country'
                })
                
                # Ajouter les colonnes manquantes
                employees_df['TitleOfCourtesy'] = 'Mr.'  # Valeur par défaut
                employees_df['ReportsTo'] = None  # Pas dans votre fichier
                
                # Pour les dates, vous pouvez les ajouter manuellement ou laisser NULL
                employees_df['BirthDate'] = None
                employees_df['HireDate'] = None
                
                # Garder seulement les colonnes nécessaires
                required_cols = ['EmployeeID', 'LastName', 'FirstName', 'Title', 
                               'TitleOfCourtesy', 'BirthDate', 'HireDate', 'Address', 
                               'City', 'Region', 'PostalCode', 'Country', 'HomePhone', 'ReportsTo']
                available_cols = [col for col in required_cols if col in employees_df.columns]
                
                if available_cols:
                    employees_df = employees_df[available_cols]
                    # Ajouter les colonnes manquantes
                    for col in required_cols:
                        if col not in employees_df.columns:
                            employees_df[col] = None
                    
                    access_data['employees_access'] = employees_df
                    print(f"  ✅ Employés Access transformés: {len(employees_df)} lignes")
                else:
                    print("  ❌ Colonnes requises non trouvées dans Employees")
                    access_data['employees_access'] = pd.DataFrame()
                    
            except Exception as e:
                print(f"  ❌ Erreur extraction employés Access: {e}")
                access_data['employees_access'] = pd.DataFrame()
            
            # 3. Extraction des Commandes (Orders)
            try:
                print("  Extraction des commandes...")
                orders_table = 'Orders'
                if orders_table not in table_list:
                    for table in table_list:
                        if 'order' in table.lower() and 'detail' not in table.lower():
                            orders_table = table
                            break

                # D'abord, inspecter les colonnes disponibles dans la table Orders
                cursor = access_conn.cursor()
                cursor.execute(f"SELECT TOP 1 * FROM [{orders_table}]")
                columns = [column[0] for column in cursor.description]
                cursor.close()

                print(f"    Colonnes disponibles dans {orders_table}: {columns}")

                # Créer un mapping des colonnes disponibles
                column_mapping = {}
                available_columns = {}

                # Mapping des colonnes attendues vers les colonnes réelles
                expected_columns = {
                    'OrderID': ['Order ID', 'ID', 'OrderID'],
                    'CustomerID': ['Customer', 'Customer ID', 'CustomerID'],
                    'EmployeeID': ['Employee', 'Employee ID', 'EmployeeID'],
                    'OrderDate': ['Order Date', 'OrderDate'],
                    'RequiredDate': ['Required Date', 'RequiredDate'],
                    'ShippedDate': ['Shipped Date', 'ShippedDate'],
                    'ShipVia': ['Ship Via', 'ShipVia'],
                    'Freight': ['Shipping Fee', 'Freight', 'Ship Fee'],
                    'ShipName': ['Ship Name', 'ShipName'],
                    'ShipAddress': ['Ship Address', 'ShipAddress'],
                    'ShipCity': ['Ship City', 'ShipCity'],
                    'ShipRegion': ['Ship State/Province', 'Ship Region', 'State/Province'],
                    'ShipPostalCode': ['Ship ZIP/Postal Code', 'Ship Postal Code', 'ZIP/Postal Code'],
                    'ShipCountry': ['Ship Country/Region', 'Ship Country', 'Country/Region']
                }

                # Trouver les colonnes disponibles
                select_columns = []
                for expected_name, possible_names in expected_columns.items():
                    found = False
                    for possible_name in possible_names:
                        if possible_name in columns:
                            select_columns.append(f"[{possible_name}] as {expected_name}")
                            available_columns[expected_name] = possible_name
                            found = True
                            break
                    if not found:
                        print(f"    ⚠️  Colonne {expected_name} non trouvée parmi {possible_names}")

                if not select_columns:
                    print("  ❌ Aucune colonne valide trouvée pour les commandes")
                    access_data['orders_access'] = pd.DataFrame()
                else:
                    # Construire la requête avec les colonnes disponibles
                    select_clause = ", ".join(select_columns)
                    query = f"SELECT {select_clause} FROM [{orders_table}] WHERE [{available_columns.get('OrderID', 'ID')}] IS NOT NULL"

                    print(f"    Requête générée: {query}")
                    orders_df = pd.read_sql(query, access_conn)
                    print(f"  ✅ Table {orders_table}: {len(orders_df)} lignes")

                try:
                    print("  Extraction des détails de commandes...")
                    order_details_table = 'Order Details'
                    if order_details_table not in table_list:
                        for table in table_list:
                            if 'order detail' in table.lower() or 'order_details' in table.lower():
                                order_details_table = table
                                break
                    
                    details_query = f"""
                        SELECT od.[Order ID] as OrderID,
                               od.[Quantity],
                               od.[Unit Price] as UnitPrice,
                               od.[Discount]
                        FROM [{order_details_table}] od
                        WHERE od.[Order ID] IS NOT NULL
                    """
                    order_details_df = pd.read_sql(details_query, access_conn)
                    print(f"  ✅ Table {order_details_table}: {len(order_details_df)} lignes")
                    
                    if not order_details_df.empty:
                        # Calculer le total par commande
                        order_details_df['LineTotal'] = order_details_df['Quantity'] * order_details_df['UnitPrice'] * (1 - order_details_df['Discount'])
                        totals = order_details_df.groupby('OrderID')['LineTotal'].sum().reset_index()
                        totals = totals.rename(columns={'LineTotal': 'TotalAmount'})
                        
                        # Fusionner avec les commandes
                        orders_df = pd.merge(orders_df, totals, on='OrderID', how='left')
                        orders_df['TotalAmount'] = orders_df['TotalAmount'].fillna(0)
                        
                        print(f"  ✅ TotalAmount calculé pour {len(orders_df)} commandes")
                    else:
                        print("  ⚠️  Aucun détail de commande trouvé")
                        orders_df['TotalAmount'] = 0
                        
                except Exception as e:
                    print(f"  ⚠️  Impossible d'extraire les détails de commandes: {e}")
                    orders_df['TotalAmount'] = 0
                
                # Nettoyer les IDs des clients et employés
                # Dans votre fichier, les clients sont "Company A", "Company B", etc.
                # On peut les convertir en IDs numériques ou garder les noms
                
                # Pour les employés, extraire le nom de l'employé
                if 'EmployeeID' in orders_df.columns:
                    # Les valeurs sont comme "Anne Hellung-Larsen"
                    # On pourrait créer un mapping vers des IDs, mais pour l'instant on garde les noms
                    pass
                
                access_data['orders_access'] = orders_df
                print(f"  ✅ Commandes Access transformées: {len(orders_df)} lignes")
                
            except Exception as e:
                print(f"  ❌ Erreur extraction commandes Access: {e}")
                access_data['orders_access'] = pd.DataFrame()
            
            access_conn.close()
            
            # Vérifier si on a des données
            has_data = False
            for key, df in access_data.items():
                if not df.empty:
                    has_data = True
                    break
            
            if not has_data:
                print("  ℹ️  Aucune donnée extraite de Access")
            
            return access_data
                
        except Exception as e:
            print(f"  ❌ Impossible d'accéder à Access: {e}")
            return {}
    
    
    def create_access_mapping(self):
        """Crée un mapping entre IDs Access et noms"""
        print("\n🗺️  CRÉATION DU MAPPING ACCESS")
        print("-"*30)
        
        mapping = {
            'customers': {},  # CustomerID -> CompanyName
            'employees': {}   # EmployeeID -> FullName
        }
        
        # 1. Lire les données Access originales pour le mapping
        try:
            access_conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            access_conn = pyodbc.connect(access_conn_str)
            
            # Mapping Customers Access
            customers_df = pd.read_sql("SELECT [ID], [Company] FROM [Customers]", access_conn)
            for _, row in customers_df.iterrows():
                customer_id = str(row['ID'])
                company_name = str(row['Company'])
                mapping['customers'][customer_id] = company_name
            
            # Mapping Employees Access  
            employees_df = pd.read_sql("SELECT [ID], [First Name], [Last Name] FROM [Employees]", access_conn)
            for _, row in employees_df.iterrows():
                employee_id = str(row['ID'])
                first_name = str(row['First Name'])
                last_name = str(row['Last Name'])
                full_name = f"{first_name} {last_name}"
                mapping['employees'][employee_id] = full_name
            
            access_conn.close()
            
            print(f"  ✅ Mapping créé: {len(mapping['customers'])} clients, {len(mapping['employees'])} employés")
            
        except Exception as e:
            print(f"  ❌ Erreur création mapping: {e}")
        
        return mapping
    
    def find_customer_key_by_access_id(self, customer_id, source_system='Access'):
        """Trouve CustomerKey à partir d'un ID Access"""
        if pd.isna(customer_id):
            return None
        
        cursor = self.dw_conn.cursor()
        
        # 1. Si c'est SQL, chercher directement
        if source_system == 'SQL':
            cursor.execute("""
                SELECT TOP 1 CustomerKey FROM DimCustomer 
                WHERE CustomerID = ? AND SourceSystem = 'SQL'
            """, (str(customer_id),))
            result = cursor.fetchone()
            if result:
                return result[0]
        
        # 2. Si c'est Access, chercher via le mapping
        elif source_system == 'Access':
            # D'abord, essayer de trouver le CustomerID Access dans DimCustomer
            access_customer_id = f"ACC-{customer_id}"
            cursor.execute("""
                SELECT TOP 1 CustomerKey FROM DimCustomer 
                WHERE CustomerID = ? AND SourceSystem = 'Access'
            """, (access_customer_id,))
            result = cursor.fetchone()
            if result:
                return result[0]
        
        return None
    
    def find_employee_key_by_access_id(self, employee_id, source_system='Access'):
        """Trouve EmployeeKey à partir d'un ID Access"""
        if pd.isna(employee_id):
            return None
        
        cursor = self.dw_conn.cursor()
        
        # 1. Si c'est SQL, chercher directement
        if source_system == 'SQL':
            try:
                emp_id = int(employee_id)
                cursor.execute("""
                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                    WHERE EmployeeID = ? AND SourceSystem = 'SQL'
                """, (emp_id,))
                result = cursor.fetchone()
                if result:
                    return result[0]
            except:
                return None
        
        # 2. Si c'est Access, chercher via le mapping
        elif source_system == 'Access':
            try:
                # EmployeeID Access est stocké comme 1000 + ID original
                access_employee_id = 1000 + int(employee_id)
                cursor.execute("""
                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                    WHERE EmployeeID = ? AND SourceSystem = 'Access'
                """, (access_employee_id,))
                result = cursor.fetchone()
                if result:
                    return result[0]
            except:
                return None
        
        return None
    
    def transform_dim_customer(self, customers_df, source_name='SQL'):
        """Transforme et nettoie la dimension Customer - VERSION AMÉLIORÉE"""
        print(f"\n👥 TRANSFORMATION DIMCUSTOMER ({source_name})")
        print("-"*30)
        
        if customers_df.empty:
            print("  ⚠️  Aucune donnée client")
            return pd.DataFrame()
        
        dim_customer = customers_df.copy()
        
        # Mapping des colonnes selon la source
        if source_name == 'Access':
            # Pour Access, les colonnes sont différentes
            column_mapping = {
                'CustomerID': 'CustomerID',
                'CompanyName': 'CompanyName',
                'ContactName': 'ContactName',
                'ContactTitle': 'ContactTitle',
                'Address': 'Address',
                'City': 'City',
                'Region': 'Region',
                'PostalCode': 'PostalCode',
                'Country': 'Country',
                'Phone': 'Phone'
            }
        else:  # SQL
            column_mapping = {
                'CustomerID': 'CustomerID',
                'CompanyName': 'CompanyName',
                'ContactName': 'ContactName',
                'ContactTitle': 'ContactTitle',
                'Address': 'Address',
                'City': 'City',
                'Region': 'Region',
                'PostalCode': 'PostalCode',
                'Country': 'Country',
                'Phone': 'Phone'
            }
        
        # Renommer les colonnes
        for old_col, new_col in column_mapping.items():
            if old_col in dim_customer.columns and new_col not in dim_customer.columns:
                dim_customer = dim_customer.rename(columns={old_col: new_col})
        
        required_cols = list(column_mapping.values())
        available_cols = [col for col in required_cols if col in dim_customer.columns]
        
        if not available_cols:
            print("  ❌ Aucune colonne valide trouvée")
            return pd.DataFrame()
        
        dim_customer = dim_customer[available_cols]
        
        # Ajouter les colonnes manquantes
        for col in required_cols:
            if col not in dim_customer.columns:
                dim_customer[col] = None
        
        dim_customer['SourceSystem'] = source_name
        
        # Nettoyage spécifique pour Access
        if source_name == 'Access':
            # Si CustomerID est numérique, le convertir en chaîne
            if 'CustomerID' in dim_customer.columns:
                dim_customer['CustomerID'] = dim_customer['CustomerID'].astype(str)
            
            # Pour Access, les IDs clients peuvent être "1", "2", etc.
            # On peut les préfixer pour éviter les conflits avec SQL
            dim_customer['CustomerID'] = 'ACC-' + dim_customer['CustomerID'].astype(str)
        
        # Nettoyage général
        if 'CustomerID' in dim_customer.columns:
            initial_count = len(dim_customer)
            dim_customer = dim_customer[dim_customer['CustomerID'].notna()]
            filtered_count = len(dim_customer)
            if filtered_count < initial_count:
                print(f"  ⚠️  {initial_count - filtered_count} lignes filtrées (CustomerID NULL)")
        
        if 'Region' in dim_customer.columns:
            dim_customer['Region'] = dim_customer['Region'].fillna('Unknown')
        if 'PostalCode' in dim_customer.columns:
            dim_customer['PostalCode'] = dim_customer['PostalCode'].fillna('Unknown')
        if 'ContactTitle' in dim_customer.columns:
            dim_customer['ContactTitle'] = dim_customer['ContactTitle'].fillna('Unknown')
        
        for col in dim_customer.columns:
            if dim_customer[col].dtype == 'object':
                dim_customer[col] = dim_customer[col].astype(str)
        
        print(f"  ✅ {len(dim_customer)} clients transformés")
        return dim_customer

    def transform_dim_employee(self, employees_df, source_name='SQL'):
        """Transforme et nettoie la dimension Employee - VERSION AMÉLIORÉE"""
        print(f"\n👨‍💼 TRANSFORMATION DIMEMPLOYEE ({source_name})")
        print("-"*30)
        
        if employees_df.empty:
            print("  ⚠️  Aucune donnée employé")
            return pd.DataFrame()
        
        dim_employee = employees_df.copy()
        
        # Mapping des colonnes selon la source
        if source_name == 'Access':
            column_mapping = {
                'EmployeeID': 'EmployeeID',
                'LastName': 'LastName',
                'FirstName': 'FirstName',
                'Title': 'Title',
                'TitleOfCourtesy': 'TitleOfCourtesy',
                'BirthDate': 'BirthDate',
                'HireDate': 'HireDate',
                'Address': 'Address',
                'City': 'City',
                'Region': 'Region',
                'PostalCode': 'PostalCode',
                'Country': 'Country',
                'HomePhone': 'HomePhone',
                'ReportsTo': 'ReportsTo'
            }
        else:  # SQL
            column_mapping = {
                'EmployeeID': 'EmployeeID',
                'LastName': 'LastName',
                'FirstName': 'FirstName',
                'Title': 'Title',
                'TitleOfCourtesy': 'TitleOfCourtesy',
                'BirthDate': 'BirthDate',
                'HireDate': 'HireDate',
                'Address': 'Address',
                'City': 'City',
                'Region': 'Region',
                'PostalCode': 'PostalCode',
                'Country': 'Country',
                'HomePhone': 'HomePhone',
                'ReportsTo': 'ReportsTo'
            }
        
        # Renommer les colonnes
        for old_col, new_col in column_mapping.items():
            if old_col in dim_employee.columns and new_col not in dim_employee.columns:
                dim_employee = dim_employee.rename(columns={old_col: new_col})
        
        required_cols = list(column_mapping.values())
        available_cols = [col for col in required_cols if col in dim_employee.columns]
        
        if not available_cols:
            print("  ❌ Aucune colonne valide trouvée")
            return pd.DataFrame()
        
        dim_employee = dim_employee[available_cols]
        
        # Ajouter les colonnes manquantes
        for col in required_cols:
            if col not in dim_employee.columns:
                dim_employee[col] = None
        
        dim_employee['SourceSystem'] = source_name
        
        # Nettoyage spécifique pour Access
        if source_name == 'Access':
            # Pour Access, les IDs employés peuvent être numériques
            if 'EmployeeID' in dim_employee.columns:
                dim_employee['EmployeeID'] = pd.to_numeric(dim_employee['EmployeeID'], errors='coerce')
                # Préfixer pour éviter les conflits
                dim_employee['EmployeeID'] = 1000 + dim_employee['EmployeeID'].fillna(0).astype(int)
        
        # Nettoyage général
        if 'EmployeeID' in dim_employee.columns:
            initial_count = len(dim_employee)
            dim_employee = dim_employee[dim_employee['EmployeeID'].notna()]
            filtered_count = len(dim_employee)
            if filtered_count < initial_count:
                print(f"  ⚠️  {initial_count - filtered_count} lignes filtrées (EmployeeID NULL)")
        
        date_cols = ['BirthDate', 'HireDate']
        for col in date_cols:
            if col in dim_employee.columns:
                dim_employee[col] = pd.to_datetime(dim_employee[col], errors='coerce')
        
        if 'Region' in dim_employee.columns:
            dim_employee['Region'] = dim_employee['Region'].fillna('Unknown')
        if 'PostalCode' in dim_employee.columns:
            dim_employee['PostalCode'] = dim_employee['PostalCode'].fillna('Unknown')
        if 'Title' in dim_employee.columns:
            dim_employee['Title'] = dim_employee['Title'].fillna('Unknown')
        
        if 'EmployeeID' in dim_employee.columns:
            dim_employee['EmployeeID'] = pd.to_numeric(dim_employee['EmployeeID'], errors='coerce')
            dim_employee = dim_employee[dim_employee['EmployeeID'].notna()]
        
        if 'ReportsTo' in dim_employee.columns:
            dim_employee['ReportsTo'] = pd.to_numeric(dim_employee['ReportsTo'], errors='coerce')
        
        print(f"  ✅ {len(dim_employee)} employés transformés")
        return dim_employee
    
    def transform_fact_orders(self, orders_df, source_name='SQL'):
        """Transforme et nettoie les faits Orders"""
        print(f"\n📦 TRANSFORMATION FACTORDERS ({source_name})")
        print("-"*30)
        
        if orders_df.empty:
            print("  ⚠️  Aucune donnée commande")
            return pd.DataFrame()
        
        fact_orders = orders_df.copy()
        
        required_cols = [
            'OrderID', 'CustomerID', 'EmployeeID', 'OrderDate', 
            'RequiredDate', 'ShippedDate', 'ShipVia', 'Freight',
            'ShipName', 'ShipAddress', 'ShipCity', 'ShipRegion',
            'ShipPostalCode', 'ShipCountry', 'TotalAmount'
        ]
        
        available_cols = [col for col in required_cols if col in fact_orders.columns]
        
        if not available_cols:
            print("  ❌ Aucune colonne valide trouvée")
            return pd.DataFrame()
        
        fact_orders = fact_orders[available_cols]
        
        for col in required_cols:
            if col not in fact_orders.columns:
                fact_orders[col] = None
        
        date_cols = ['OrderDate', 'RequiredDate', 'ShippedDate']
        for col in date_cols:
            if col in fact_orders.columns:
                fact_orders[col] = pd.to_datetime(fact_orders[col], errors='coerce')
        
        fact_orders['IsDelivered'] = fact_orders['ShippedDate'].notna().astype(int)
        
        if 'ShippedDate' in fact_orders.columns and 'RequiredDate' in fact_orders.columns:
            fact_orders['DeliveryDelayDays'] = np.where(
                fact_orders['ShippedDate'].notna() & fact_orders['RequiredDate'].notna(),
                (fact_orders['ShippedDate'] - fact_orders['RequiredDate']).dt.days,
                None
            )
        else:
            fact_orders['DeliveryDelayDays'] = None
        
        fact_orders['SourceSystem'] = source_name
        
        if 'Freight' in fact_orders.columns:
            fact_orders['Freight'] = pd.to_numeric(fact_orders['Freight'], errors='coerce').fillna(0)
        if 'TotalAmount' in fact_orders.columns:
            fact_orders['TotalAmount'] = pd.to_numeric(fact_orders['TotalAmount'], errors='coerce').fillna(0)
        
        print(f"  ✅ {len(fact_orders)} commandes transformées")
        return fact_orders
    
    def load_dimensions_to_dw(self, dim_customer, dim_employee):
        """Charge les dimensions dans le DW avec gestion des doublons"""
        print("\n📤 CHARGEMENT DES DIMENSIONS")
        print("-"*30)
        
        if self.dw_conn is None:
            print("  ❌ Pas de connexion au DW")
            return
        
        self._ensure_dimcustomer_table_exists()
        self._ensure_dimemployee_table_exists()
        
        if not dim_customer.empty:
            print("  📋 Chargement DimCustomer...")
            try:
                existing_query = "SELECT CustomerID, SourceSystem FROM DimCustomer"
                existing_customers = pd.read_sql(existing_query, self.dw_conn)
                
                if not existing_customers.empty:
                    dim_customer['composite_key'] = dim_customer['CustomerID'].astype(str) + '_' + dim_customer['SourceSystem'].astype(str)
                    existing_customers['composite_key'] = existing_customers['CustomerID'].astype(str) + '_' + existing_customers['SourceSystem'].astype(str)
                    
                    new_customers = dim_customer[~dim_customer['composite_key'].isin(existing_customers['composite_key'])]
                    dim_customer = new_customers.drop('composite_key', axis=1, errors='ignore')
                
                if not dim_customer.empty:
                    cursor = self.dw_conn.cursor()
                    inserted_count = 0
                    
                    for _, row in dim_customer.iterrows():
                        try:
                            customer_id = str(row.get('CustomerID', '')) if pd.notna(row.get('CustomerID')) else ''
                            company_name = str(row.get('CompanyName', '')) if pd.notna(row.get('CompanyName')) else ''
                            
                            if not customer_id:
                                continue
                                
                            cursor.execute("""
                                INSERT INTO DimCustomer (CustomerID, CompanyName, ContactName, ContactTitle, 
                                Address, City, Region, PostalCode, Country, Phone, SourceSystem)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, 
                            customer_id,
                            company_name,
                            str(row.get('ContactName', '')) if pd.notna(row.get('ContactName')) else '',
                            str(row.get('ContactTitle', '')) if pd.notna(row.get('ContactTitle')) else '',
                            str(row.get('Address', '')) if pd.notna(row.get('Address')) else '',
                            str(row.get('City', '')) if pd.notna(row.get('City')) else '',
                            str(row.get('Region', '')) if pd.notna(row.get('Region')) else '',
                            str(row.get('PostalCode', '')) if pd.notna(row.get('PostalCode')) else '',
                            str(row.get('Country', '')) if pd.notna(row.get('Country')) else '',
                            str(row.get('Phone', '')) if pd.notna(row.get('Phone')) else '',
                            str(row.get('SourceSystem', 'Unknown')) if pd.notna(row.get('SourceSystem')) else 'Unknown')
                            
                            inserted_count += 1
                            
                        except Exception as row_error:
                            print(f"    ⚠️  Erreur ligne {_}: {row_error}")
                            continue
                    
                    self.dw_conn.commit()
                    cursor.close()
                    
                    print(f"    ✅ {inserted_count} nouveaux clients ajoutés")
                else:
                    print("    ℹ️  Tous les clients existent déjà")
                    
            except Exception as e:
                print(f"    ❌ Erreur chargement DimCustomer: {e}")
        else:
            print("  ℹ️  Aucun client à charger")
        
        if not dim_employee.empty:
            print("  📋 Chargement DimEmployee...")
            try:
                existing_query = "SELECT EmployeeID, SourceSystem FROM DimEmployee"
                existing_employees = pd.read_sql(existing_query, self.dw_conn)
                
                if not existing_employees.empty:
                    dim_employee['composite_key'] = dim_employee['EmployeeID'].astype(str) + '_' + dim_employee['SourceSystem'].astype(str)
                    existing_employees['composite_key'] = existing_employees['EmployeeID'].astype(str) + '_' + existing_employees['SourceSystem'].astype(str)
                    
                    new_employees = dim_employee[~dim_employee['composite_key'].isin(existing_employees['composite_key'])]
                    dim_employee = new_employees.drop('composite_key', axis=1, errors='ignore')
                
                if not dim_employee.empty:
                    cursor = self.dw_conn.cursor()
                    inserted_count = 0
                    
                    for _, row in dim_employee.iterrows():
                        try:
                            employee_id = int(row.get('EmployeeID', 0)) if pd.notna(row.get('EmployeeID')) else 0
                            
                            if employee_id == 0:
                                continue
                            
                            reports_to = row.get('ReportsTo')
                            if pd.isna(reports_to):
                                reports_to = None
                            else:
                                reports_to = int(reports_to)
                            
                            cursor.execute("""
                                INSERT INTO DimEmployee (EmployeeID, LastName, FirstName, Title, 
                                TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, 
                                PostalCode, Country, HomePhone, ReportsTo, SourceSystem)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            employee_id,
                            str(row.get('LastName', '')) if pd.notna(row.get('LastName')) else '',
                            str(row.get('FirstName', '')) if pd.notna(row.get('FirstName')) else '',
                            str(row.get('Title', '')) if pd.notna(row.get('Title')) else '',
                            str(row.get('TitleOfCourtesy', '')) if pd.notna(row.get('TitleOfCourtesy')) else '',
                            row.get('BirthDate') if pd.notna(row.get('BirthDate')) else None,
                            row.get('HireDate') if pd.notna(row.get('HireDate')) else None,
                            str(row.get('Address', '')) if pd.notna(row.get('Address')) else '',
                            str(row.get('City', '')) if pd.notna(row.get('City')) else '',
                            str(row.get('Region', '')) if pd.notna(row.get('Region')) else '',
                            str(row.get('PostalCode', '')) if pd.notna(row.get('PostalCode')) else '',
                            str(row.get('Country', '')) if pd.notna(row.get('Country')) else '',
                            str(row.get('HomePhone', '')) if pd.notna(row.get('HomePhone')) else '',
                            reports_to,
                            str(row.get('SourceSystem', 'Unknown')) if pd.notna(row.get('SourceSystem')) else 'Unknown')
                            
                            inserted_count += 1
                            
                        except Exception as row_error:
                            print(f"    ⚠️  Erreur ligne {_}: {row_error}")
                            continue
                    
                    self.dw_conn.commit()
                    cursor.close()
                    
                    print(f"    ✅ {inserted_count} nouveaux employés ajoutés")
                else:
                    print("    ℹ️  Tous les employés existent déjà")
                    
            except Exception as e:
                print(f"    ❌ Erreur chargement DimEmployee: {e}")
        else:
            print("  ℹ️  Aucun employé à charger")
    
    def _ensure_dimcustomer_table_exists(self):
        """S'assure que la table DimCustomer existe"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimCustomer' AND xtype='U')
                BEGIN
                    CREATE TABLE DimCustomer (
                        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
                        CustomerID VARCHAR(10) NOT NULL,
                        CompanyName VARCHAR(100) NOT NULL,
                        ContactName VARCHAR(100),
                        ContactTitle VARCHAR(100),
                        Address VARCHAR(200),
                        City VARCHAR(50),
                        Region VARCHAR(50),
                        PostalCode VARCHAR(20),
                        Country VARCHAR(50),
                        Phone VARCHAR(30),
                        SourceSystem VARCHAR(20),
                        UNIQUE(CustomerID, SourceSystem)
                    );
                END
            """)
            self.dw_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"⚠️  Erreur création DimCustomer: {e}")
    
    def _ensure_dimemployee_table_exists(self):
        """S'assure que la table DimEmployee existe"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimEmployee' AND xtype='U')
                BEGIN
                    CREATE TABLE DimEmployee (
                        EmployeeKey INT IDENTITY(1,1) PRIMARY KEY,
                        EmployeeID INT NOT NULL,
                        LastName VARCHAR(50) NOT NULL,
                        FirstName VARCHAR(50) NOT NULL,
                        Title VARCHAR(100),
                        TitleOfCourtesy VARCHAR(25),
                        BirthDate DATE,
                        HireDate DATE,
                        Address VARCHAR(200),
                        City VARCHAR(50),
                        Region VARCHAR(50),
                        PostalCode VARCHAR(20),
                        Country VARCHAR(50),
                        HomePhone VARCHAR(30),
                        ReportsTo INT,
                        SourceSystem VARCHAR(20),
                        UNIQUE(EmployeeID, SourceSystem)
                    );
                END
            """)
            self.dw_conn.commit()
            cursor.close()
        except Exception as e:
            print(f"⚠️  Erreur création DimEmployee: {e}")
    
    def _ensure_factorders_table_exists(self):
        """S'assure que la table FactOrders existe"""
        try:
            cursor = self.dw_conn.cursor()
            cursor.execute("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FactOrders' AND xtype='U')
                BEGIN
                    CREATE TABLE FactOrders (
                        FactOrderKey INT IDENTITY(1,1) PRIMARY KEY,
                        OrderID INT NOT NULL,
                        CustomerKey INT,
                        EmployeeKey INT,
                        OrderDateKey INT,
                        OrderDate DATE,
                        RequiredDate DATE,
                        ShippedDate DATE,
                        ShipVia INT,
                        Freight DECIMAL(10,2),
                        ShipName VARCHAR(100),
                        ShipAddress VARCHAR(200),
                        ShipCity VARCHAR(50),
                        ShipRegion VARCHAR(50),
                        ShipPostalCode VARCHAR(20),
                        ShipCountry VARCHAR(50),
                        TotalAmount DECIMAL(10,2),
                        IsDelivered BIT,
                        DeliveryDelayDays INT,
                        SourceSystem VARCHAR(20),
                        FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
                        FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey),
                        FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey)
                    );
                    
                    -- Créer des index pour les performances
                    CREATE INDEX IX_FactOrders_OrderDateKey ON FactOrders(OrderDateKey);
                    CREATE INDEX IX_FactOrders_CustomerKey ON FactOrders(CustomerKey);
                    CREATE INDEX IX_FactOrders_EmployeeKey ON FactOrders(EmployeeKey);
                END
            """)
            self.dw_conn.commit()
            cursor.close()
            print("  ✅ Table FactOrders vérifiée/créée")
        except Exception as e:
            print(f"  ❌ Erreur création FactOrders: {e}")
    
    def load_facts_to_dw(self, fact_orders):
        """Charge les faits dans le DW avec recherche intelligente des clés"""
        print("\n📤 CHARGEMENT DES FAITS")
        print("-"*30)
    
        if self.dw_conn is None or fact_orders.empty:
            print("  ℹ️  Aucune donnée à charger")
            return
    
        # Créer le mapping Access
        access_mapping = self.create_access_mapping()
        
        # Vérifier/créer la table
        self._ensure_factorders_table_exists()
    
        print("  🔍 Recherche intelligente des clés de dimension...")
    
        try:
            cursor = self.dw_conn.cursor()
            
            # Filtrer les commandes existantes
            existing_query = "SELECT OrderID, SourceSystem FROM FactOrders"
            existing_orders = pd.read_sql(existing_query, self.dw_conn)
    
            if not existing_orders.empty:
                fact_orders['composite_key'] = fact_orders['OrderID'].astype(str) + '_' + fact_orders['SourceSystem'].astype(str)
                existing_orders['composite_key'] = existing_orders['OrderID'].astype(str) + '_' + existing_orders['SourceSystem'].astype(str)
    
                new_orders = fact_orders[~fact_orders['composite_key'].isin(existing_orders['composite_key'])]
                fact_orders = new_orders.drop('composite_key', axis=1, errors='ignore')
    
            if fact_orders.empty:
                print("  ℹ️  Toutes les commandes existent déjà")
                return
    
            # Préparer DateKey
            fact_orders_with_keys = fact_orders.copy()
            if 'OrderDate' in fact_orders_with_keys.columns:
                fact_orders_with_keys['OrderDate'] = pd.to_datetime(fact_orders_with_keys['OrderDate'], errors='coerce')
                fact_orders_with_keys['OrderDateKey'] = fact_orders_with_keys['OrderDate'].dt.strftime('%Y%m%d').astype('Int64')
    
            # Insérer avec recherche intelligente
            inserted_count = 0
            error_count = 0
            
            for idx, row in fact_orders_with_keys.iterrows():
                try:
                    source_system = row.get('SourceSystem', 'SQL')
                    order_id = int(row.get('OrderID', 0)) if pd.notna(row.get('OrderID')) else 0
                    
                    if order_id == 0:
                        continue
                    
                    # DateKey (OBLIGATOIRE)
                    order_date = row.get('OrderDate')
                    order_date_key = None
                    if pd.notna(order_date):
                        try:
                            order_date_key = int(pd.to_datetime(order_date).strftime('%Y%m%d'))
                        except:
                            pass
                    
                    if order_date_key is None:
                        print(f"    ⚠️  Commande {order_id} ignorée: pas de OrderDate")
                        continue
                    
                    # CustomerKey - RECHERCHE INTELLIGENTE
                    customer_key = None
                    customer_id = row.get('CustomerID')
                    
                    if source_system == 'SQL':
                        # Pour SQL Server
                        if pd.notna(customer_id):
                            cursor.execute("""
                                SELECT TOP 1 CustomerKey FROM DimCustomer 
                                WHERE CustomerID = ? AND SourceSystem = 'SQL'
                            """, (str(customer_id),))
                            result = cursor.fetchone()
                            if result:
                                customer_key = result[0]
                            else:
                                print(f"    ⚠️  CustomerID SQL {customer_id} non trouvé")
                    
                    elif source_system == 'Access':
                        # Pour Access
                        if pd.notna(customer_id):
                            # 1. Essayer avec le format ACC-XX
                            access_customer_id = f"ACC-{customer_id}"
                            cursor.execute("""
                                SELECT TOP 1 CustomerKey FROM DimCustomer 
                                WHERE CustomerID = ? AND SourceSystem = 'Access'
                            """, (access_customer_id,))
                            result = cursor.fetchone()
                            
                            if result:
                                customer_key = result[0]
                            else:
                                # 2. Chercher par nom de compagnie via le mapping
                                company_name = access_mapping['customers'].get(str(customer_id))
                                if company_name:
                                    cursor.execute("""
                                        SELECT TOP 1 CustomerKey FROM DimCustomer 
                                        WHERE CompanyName LIKE ? AND SourceSystem = 'Access'
                                    """, (f"%{company_name}%",))
                                    result = cursor.fetchone()
                                    if result:
                                        customer_key = result[0]
                                else:
                                    print(f"    ⚠️  CustomerID Access {customer_id} non trouvé")
                    
                    # EmployeeKey - RECHERCHE INTELLIGENTE
                    employee_key = None
                    employee_id = row.get('EmployeeID')
                    
                    if source_system == 'SQL':
                        # Pour SQL Server
                        if pd.notna(employee_id):
                            try:
                                emp_id = int(employee_id)
                                cursor.execute("""
                                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                    WHERE EmployeeID = ? AND SourceSystem = 'SQL'
                                """, (emp_id,))
                                result = cursor.fetchone()
                                if result:
                                    employee_key = result[0]
                                else:
                                    print(f"    ⚠️  EmployeeID SQL {employee_id} non trouvé")
                            except:
                                pass
                    
                    elif source_system == 'Access':
                        # Pour Access
                        if pd.notna(employee_id):
                            try:
                                # 1. Essayer avec 1000 + ID
                                access_employee_id = 1000 + int(employee_id)
                                cursor.execute("""
                                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                    WHERE EmployeeID = ? AND SourceSystem = 'Access'
                                """, (access_employee_id,))
                                result = cursor.fetchone()
                                
                                if result:
                                    employee_key = result[0]
                                else:
                                    # 2. Chercher par nom via le mapping
                                    full_name = access_mapping['employees'].get(str(employee_id))
                                    if full_name:
                                        # Essayer différents formats de nom
                                        cursor.execute("""
                                            SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                            WHERE (FirstName + ' ' + LastName LIKE ? 
                                                   OR LastName + ', ' + FirstName LIKE ?)
                                            AND SourceSystem = 'Access'
                                        """, (f"%{full_name}%", f"%{full_name}%"))
                                        result = cursor.fetchone()
                                        if result:
                                            employee_key = result[0]
                                    else:
                                        print(f"    ⚠️  EmployeeID Access {employee_id} non trouvé")
                            except:
                                pass
                    
                    # VÉRIFICATION : Si pas de clés trouvées, on insère quand même NULL
                    # Mais on affiche un avertissement
                    if customer_key is None:
                        print(f"    ℹ️  Commande {order_id}: CustomerKey = NULL (ID: {customer_id})")
                    if employee_key is None:
                        print(f"    ℹ️  Commande {order_id}: EmployeeKey = NULL (ID: {employee_id})")
                    
                    # Insérer la commande
                    cursor.execute("""
                        INSERT INTO FactOrders (
                            OrderID, CustomerKey, EmployeeKey, OrderDateKey,
                            OrderDate, ShippedDate, ShipVia, Freight,
                            ShipName, ShipAddress, ShipCity, ShipRegion,
                            ShipPostalCode, ShipCountry, TotalAmount,
                            IsDelivered, SourceSystem
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    order_id,
                    customer_key,  # Peut être NULL
                    employee_key,  # Peut être NULL
                    order_date_key,
                    order_date if pd.notna(order_date) else None,
                    row.get('ShippedDate') if pd.notna(row.get('ShippedDate')) else None,
                    int(row.get('ShipVia', 0)) if pd.notna(row.get('ShipVia')) else 0,
                    float(row.get('Freight', 0)) if pd.notna(row.get('Freight')) else 0.0,
                    str(row.get('ShipName', '')) if pd.notna(row.get('ShipName')) else '',
                    str(row.get('ShipAddress', '')) if pd.notna(row.get('ShipAddress')) else '',
                    str(row.get('ShipCity', '')) if pd.notna(row.get('ShipCity')) else '',
                    str(row.get('ShipRegion', '')) if pd.notna(row.get('ShipRegion')) else '',
                    str(row.get('ShipPostalCode', '')) if pd.notna(row.get('ShipPostalCode')) else '',
                    str(row.get('ShipCountry', '')) if pd.notna(row.get('ShipCountry')) else '',
                    float(row.get('TotalAmount', 0)) if pd.notna(row.get('TotalAmount')) else 0.0,
                    int(row.get('IsDelivered', 0)) if pd.notna(row.get('IsDelivered')) else 0,
                    str(source_system)
                    )
                    
                    inserted_count += 1
                    
                    if inserted_count % 20 == 0:
                        print(f"    {inserted_count} commandes insérées...")
                        
                except Exception as row_error:
                    error_count += 1
                    if error_count <= 10:
                        print(f"    ⚠️  Erreur ligne {idx}: {str(row_error)[:80]}")
                    continue
            
            self.dw_conn.commit()
            cursor.close()
            
            print(f"\n  ✅ {inserted_count} commandes chargées dans FactOrders")
            print(f"  ℹ️  Résumé:")
            print(f"    - Commandes avec CustomerKey: {inserted_count - error_count}")
            print(f"    - Commandes avec EmployeeKey: {inserted_count - error_count}")
            if error_count > 0:
                print(f"    - Erreurs: {error_count}")
                
        except Exception as e:
            print(f"  ❌ Erreur chargement: {e}")
            import traceback
            traceback.print_exc()
    
    def make_factorders_not_null(self):
        """Après chargement, rend les colonnes NOT NULL et nettoie"""
        print("\n🔧 NETTOYAGE FINAL DE FACTORDERS")
        print("-"*30)
        
        try:
            cursor = self.dw_conn.cursor()
            
            # 1. Supprimer les lignes sans CustomerKey ou EmployeeKey
            cursor.execute("""
                DELETE FROM FactOrders 
                WHERE CustomerKey IS NULL OR EmployeeKey IS NULL
            """)
            deleted = cursor.rowcount
            print(f"  ✅ {deleted} lignes sans clés supprimées")
            
            # 2. Rendre les colonnes NOT NULL
            cursor.execute("""
                ALTER TABLE FactOrders ALTER COLUMN CustomerKey INT NOT NULL;
                ALTER TABLE FactOrders ALTER COLUMN EmployeeKey INT NOT NULL;
            """)
            
            self.dw_conn.commit()
            cursor.close()
            print("  ✅ Colonnes rendues NOT NULL")
            
        except Exception as e:
            print(f"  ❌ Erreur nettoyage: {e}")
    
    def run_full_etl(self):
        """Exécute le processus ETL complet"""
        print("\n" + "="*50)
        print("🚀 DÉMARRAGE PROCESSUS ETL COMPLET")
        print("="*50)
        
        try:
            # Étape 1: Créer/remplir DimDate
            self.create_dim_date(1990, 2025)
            
            # Étape 2: Extraire depuis SQL Server
            sql_data = self.extract_from_sql_server()
            
            # Étape 3: Transformer les données SQL
            dim_customer_sql = self.transform_dim_customer(sql_data.get('customers', pd.DataFrame()), 'SQL')
            dim_employee_sql = self.transform_dim_employee(sql_data.get('employees', pd.DataFrame()), 'SQL')
            fact_orders_sql = self.transform_fact_orders(sql_data.get('orders', pd.DataFrame()), 'SQL')
            
            # Étape 4: Extraire depuis Access (optionnel)
            access_data = self.extract_from_access()
            
            if access_data:
                dim_customer = pd.concat([
                    dim_customer_sql,
                    self.transform_dim_customer(access_data.get('customers_access', pd.DataFrame()), 'Access')
                ], ignore_index=True)
                
                dim_employee = pd.concat([
                    dim_employee_sql,
                    self.transform_dim_employee(access_data.get('employees_access', pd.DataFrame()), 'Access')
                ], ignore_index=True)
                
                fact_orders = pd.concat([
                    fact_orders_sql,
                    self.transform_fact_orders(access_data.get('orders_access', pd.DataFrame()), 'Access')
                ], ignore_index=True)
            else:
                dim_customer = dim_customer_sql
                dim_employee = dim_employee_sql
                fact_orders = fact_orders_sql
            
            # Étape 5: Charger les dimensions
            self.load_dimensions_to_dw(dim_customer, dim_employee)
            
            # Étape 6: Charger les faits (AJOUTÉ)
            self.load_facts_to_dw(fact_orders)
            
            # Étape 7: Sauvegarder pour dashboard
            print("\n🎯 PRÉPARATION POUR DASHBOARD")
            print("-"*30)
            
            try:
                import os
                if not os.path.exists('data'):
                    os.makedirs('data')
                fact_orders.to_csv('data/fact_orders_transformed.csv', index=False)
                print("  ✅ Données sauvegardées dans data/fact_orders_transformed.csv")
            except Exception as e:
                print(f"  ⚠️  Impossible de sauvegarder les données: {e}")
            
            self.show_summary()
            
            print("\n" + "="*50)
            print("🎉 PROCESSUS ETL TERMINÉ AVEC SUCCÈS!")
            print("="*50)
            
        except Exception as e:
            print(f"\n❌ ERREUR CRITIQUE DANS L'ETL: {e}")
            raise
    
    def show_summary(self):
        """Affiche un résumé du data warehouse"""
        print("\n📊 RÉSUMÉ DATA WAREHOUSE")
        print("-"*30)
        
        if self.dw_conn is None:
            print("❌ Pas de connexion au DW")
            return
        
        tables = ['DimDate', 'DimCustomer', 'DimEmployee', 'FactOrders']
        for table in tables:
            try:
                cursor = self.dw_conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                cursor.close()
                print(f"  {table}: {count} lignes")
            except Exception as e:
                print(f"  {table}: TABLE NON DISPONIBLE")
    
    def close_connections(self):
        """Ferme toutes les connexions"""
        print("\n🔌 FERMETURE DES CONNEXIONS")
        print("-"*30)
        
        try:
            if hasattr(self, 'source_conn') and self.source_conn:
                self.source_conn.close()
                print("  ✅ Connexion source fermée")
        except:
            print("  ⚠️  Erreur fermeture connexion source")
        
        try:
            if hasattr(self, 'dw_conn') and self.dw_conn:
                self.dw_conn.close()
                print("  ✅ Connexion DW fermée")
        except:
            print("  ⚠️  Erreur fermeture connexion DW")
        
        try:
            if hasattr(self, 'dw_engine') and self.dw_engine:
                self.dw_engine.dispose()
                print("  ✅ Engine SQLAlchemy fermé")
        except:
            print("  ⚠️  Erreur fermeture engine SQLAlchemy")

# Exécution principale
if __name__ == "__main__":
    etl = None
    try:
        etl = Northwind()
        etl.run_full_etl()
    except KeyboardInterrupt:
        print("\n\n⏹️  ETL interrompu par l'utilisateur")
    except Exception as e:
        print(f"\n\n❌ ERREUR FATALE: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if etl:
            etl.close_connections()
        print("\n" + "="*50)
        print("🏁 EXÉCUTION TERMINÉE")
        print("="*50)