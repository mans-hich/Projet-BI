# create_database.py
from DatabaseConfig import DatabaseConfig, create_sql_connection
import pyodbc
from connect import connect_sql_server, connect_data_werehouse

def create_datawarehouse():
    """Crée la base de données du data warehouse si elle n'existe pas"""
    try:
        # Connexion au serveur master pour créer la base
        master_config = DatabaseConfig.DW_SERVER.copy()
        master_config['database'] = 'master'
        
        conn = create_sql_connection()
        cursor = conn.cursor()
        
        # Créer la base si elle n'existe pas
        db_name = DatabaseConfig.DW_SERVER['database']
        cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}')
            BEGIN
                CREATE DATABASE [{db_name}];
                PRINT 'Base de données {db_name} créée avec succès.';
            END
            ELSE
                PRINT 'Base de données {db_name} existe déjà.';
        """)
        
        conn.commit()
        print(f"✅ Base de données '{db_name}' prête.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"❌ Erreur création base de données: {e}")

def create_dw_schema():
    """Crée les tables du data warehouse"""
    conn = connect_data_werehouse()
    cursor = conn.cursor()
    
    # 1. Table DimDate
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='DimDate' AND xtype='U')
        BEGIN
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
            );
            PRINT 'Table DimDate créée.';
        END
    """)
    
    # 2. Table DimCustomer
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
            PRINT 'Table DimCustomer créée.';
        END
    """)
    
    # 3. Table DimEmployee
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
            PRINT 'Table DimEmployee créée.';
        END
    """)
    
    # 4. Table FactOrders
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='FactOrders' AND xtype='U')
        BEGIN
            CREATE TABLE FactOrders (
                OrderKey INT IDENTITY(1,1) PRIMARY KEY,
                OrderID INT NOT NULL,
                CustomerKey INT NOT NULL,
                EmployeeKey INT NOT NULL,
                OrderDateKey INT NOT NULL,
                RequiredDateKey INT,
                ShippedDateKey INT,
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
                IsDelivered BIT DEFAULT 0,
                DeliveryDelayDays INT,
                TotalAmount DECIMAL(15,2),
                SourceSystem VARCHAR(20),
                
                FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
                FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey),
                FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey),
                FOREIGN KEY (RequiredDateKey) REFERENCES DimDate(DateKey),
                FOREIGN KEY (ShippedDateKey) REFERENCES DimDate(DateKey)
            );
            PRINT 'Table FactOrders créée.';
        END
    """)
    
    # Créer des index pour améliorer les performances
    cursor.execute("""
        CREATE INDEX IX_FactOrders_Dates ON FactOrders (OrderDateKey, RequiredDateKey, ShippedDateKey);
        CREATE INDEX IX_FactOrders_Customer ON FactOrders (CustomerKey);
        CREATE INDEX IX_FactOrders_Employee ON FactOrders (EmployeeKey);
        CREATE INDEX IX_FactOrders_IsDelivered ON FactOrders (IsDelivered);
    """)
    
    conn.commit()
    print("✅ Schema du data warehouse créé avec succès.")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_datawarehouse()
    create_dw_schema()