import pyodbc
from DatabaseConfig import DatabaseConfig


def init_datawarehouse():
    """Create the data warehouse database if it does not exist."""
    try:
        connection = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};"
            "DATABASE=master;"
            "Trusted_Connection=yes;",
            autocommit=True
        )
        cur = connection.cursor()

        cur.execute(
            "SELECT 1 FROM sys.databases WHERE name = ?",
            DatabaseConfig.TARGET_DATABASE
        )

        if cur.fetchone():
            print(f"ℹ️ Database '{DatabaseConfig.TARGET_DATABASE}' already present")
            cur.close()
            connection.close()
            return True

        cur.execute(f"CREATE DATABASE {DatabaseConfig.TARGET_DATABASE}")
        connection.commit()
        print(f"✅ Database '{DatabaseConfig.TARGET_DATABASE}' successfully created")

        cur.close()
        connection.close()
        return True

    except Exception as err:
        print(f"❌ Database creation failed: {err}")
        return False


def build_dw_tables():
    """Create dimension and fact tables."""
    try:
        connection = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};"
            f"DATABASE={DatabaseConfig.TARGET_DATABASE};"
            "Trusted_Connection=yes;"
        )
        cur = connection.cursor()

        # ---------- DimDate ----------
        cur.execute("""
            IF OBJECT_ID('DimDate', 'U') IS NULL
            CREATE TABLE DimDate (
                DateKey INT PRIMARY KEY,
                FullDate DATE NOT NULL,
                Year INT NOT NULL,
                Quarter INT NOT NULL,
                Month INT NOT NULL,
                Day INT NOT NULL,
                MonthLabel VARCHAR(20),
                WeekDayLabel VARCHAR(20),
                WeekendFlag BIT,
                CONSTRAINT UQ_DimDate UNIQUE (FullDate)
            )
        """)
        print("✔ DimDate ready")

        # ---------- DimCustomer ----------
        cur.execute("""
            IF OBJECT_ID('DimCustomer', 'U') IS NULL
            CREATE TABLE DimCustomer (
                CustomerKey INT IDENTITY PRIMARY KEY,
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
                CONSTRAINT UQ_Customer UNIQUE (CustomerID, SourceSystem)
            )
        """)
        print("✔ DimCustomer ready")

        # ---------- DimEmployee ----------
        cur.execute("""
            IF OBJECT_ID('DimEmployee', 'U') IS NULL
            CREATE TABLE DimEmployee (
                EmployeeKey INT IDENTITY PRIMARY KEY,
                EmployeeID INT NOT NULL,
                LastName VARCHAR(50),
                FirstName VARCHAR(50),
                Title VARCHAR(100),
                CourtesyTitle VARCHAR(25),
                BirthDate DATE,
                HireDate DATE,
                Address VARCHAR(200),
                City VARCHAR(50),
                Region VARCHAR(50),
                PostalCode VARCHAR(20),
                Country VARCHAR(50),
                Phone VARCHAR(30),
                ReportsTo INT,
                SourceSystem VARCHAR(20),
                CONSTRAINT UQ_Employee UNIQUE (EmployeeID, SourceSystem)
            )
        """)
        print("✔ DimEmployee ready")

        # ---------- FactOrders ----------
        cur.execute("""
            IF OBJECT_ID('FactOrders', 'U') IS NULL
            CREATE TABLE FactOrders (
                FactOrderKey INT IDENTITY PRIMARY KEY,
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
                Delivered BIT,
                DeliveryDelay INT,
                SourceSystem VARCHAR(20)
            )
        """)
        print("✔ FactOrders ready")

        connection.commit()
        cur.close()
        connection.close()
        print("✅ Schema creation completed")
        return True

    except Exception as err:
        print(f"❌ Schema creation error: {err}")
        return False


def configure_constraints():
    """Add foreign keys and indexes."""
    try:
        connection = pyodbc.connect(
            f"DRIVER={{SQL Server}};"
            f"SERVER={DatabaseConfig.SQL_SERVER_INSTANCE};"
            f"DATABASE={DatabaseConfig.TARGET_DATABASE};"
            "Trusted_Connection=yes;"
        )
        cur = connection.cursor()

        relations = [
            ("FK_FactOrders_Customer", "CustomerKey", "DimCustomer", "CustomerKey"),
            ("FK_FactOrders_Employee", "EmployeeKey", "DimEmployee", "EmployeeKey"),
            ("FK_FactOrders_Date", "OrderDateKey", "DimDate", "DateKey"),
        ]

        for fk, col, dim, dim_col in relations:
            try:
                cur.execute(f"""
                    IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = '{fk}')
                    ALTER TABLE FactOrders
                    ADD CONSTRAINT {fk}
                    FOREIGN KEY ({col}) REFERENCES {dim}({dim_col})
                """)
                print(f"✔ {fk} applied")
            except:
                print(f"ℹ️ {fk} already exists")

        indexes = ["OrderDateKey", "CustomerKey", "EmployeeKey"]
        for col in indexes:
            try:
                cur.execute(f"""
                    IF NOT EXISTS (
                        SELECT 1 FROM sys.indexes WHERE name = 'IX_FactOrders_{col}'
                    )
                    CREATE INDEX IX_FactOrders_{col} ON FactOrders({col})
                """)
                print(f"✔ Index on {col} created")
            except:
                print(f"ℹ️ Index on {col} already exists")

        connection.commit()
        cur.close()
        connection.close()
        print("✅ Constraints and indexes configured")
        return True

    except Exception as err:
        print(f"❌ Constraint configuration failed: {err}")
        return False


if __name__ == "__main__":
    print("🚀 Initializing Data Warehouse...")
    if init_datawarehouse():
        if build_dw_tables():
            configure_constraints()
            print("🎯 Data Warehouse is fully operational")
