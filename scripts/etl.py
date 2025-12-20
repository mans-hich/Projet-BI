import pandas as pd
import numpy as np
import pyodbc
from DatabaseConfig import DatabaseConfig, connect_to_database, validate_connections
import create_datawarehouse


class etl:

    def __init__(self):
        print("=" * 50)
        print("NORTHWIND DATA INTEGRATION INITIALIZATION")
        print("=" * 50)

        # Establish connection to operational database
        print("\n1. Connecting to operational SQL database...")
        self.source_connection = connect_to_database(DatabaseConfig.SOURCE_DATABASE)
        self.target_connection = connect_to_database(DatabaseConfig.TARGET_DATABASE)

        if self.source_connection is None:
            raise Exception("Connection failed: Operational database unreachable")
        print("   ✅ Operational database connection established")

        # Initialize data warehouse
        print("\n2. Verifying data warehouse structure...")
        try:
            if create_datawarehouse.initialize_warehouse():
                print("   ✅ Data warehouse verified")
                if create_datawarehouse.setup_warehouse_schema():
                    print("   ✅ Warehouse schema configured")
                else:
                    print("   ⚠️ Schema may already exist")
            else:
                print("   ⚠️ Warehouse initialization incomplete")
        except Exception as e:
            print(f"   ⚠️ Warehouse setup issue: {e}")

        # Connect to data warehouse
        print("\n3. Connecting to data warehouse...")
        self.warehouse_connection = validate_connections()

        if self.warehouse_connection is None:
            raise Exception("Connection failed: Data warehouse unreachable")
        print("   ✅ Data warehouse connection established")

        print("\n" + "=" * 50)
        print("✅ ALL CONNECTIONS SUCCESSFUL")
        print("=" * 50)

    # UTILITY METHODS
    def table_exists_check(self, table_identifier):
        try:
            cursor = self.warehouse_connection.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = ?
            """, table_identifier)
            exists_flag = cursor.fetchone()[0] > 0
            cursor.close()
            return exists_flag
        except Exception as e:
            print(f"⚠️ Table verification error for {table_identifier}: {e}")
            return False

    def populate_date_dimension(self, start_year=1990, end_year=2025):
        print("\nDATE DIMENSION POPULATION")
        print("-" * 30)

        self._verify_date_dimension_structure()

        try:
            record_count = self.warehouse_connection.execute("SELECT COUNT(*) FROM DimDate").fetchone()[0]
            if record_count > 0:
                print(f" Date dimension contains {record_count:,} entries")
                return pd.DataFrame()
        except:
            pass

        print(f" Generating date range {start_year} through {end_year}...")

        date_sequence = pd.date_range(start=f'{start_year}-01-01', end=f'{end_year}-12-31', freq='D')
        date_dimension = pd.DataFrame({
            'DateKey': date_sequence.strftime('%Y%m%d').astype(int),
            'Date': date_sequence.date,
            'Year': date_sequence.year,
            'Quarter': date_sequence.quarter,
            'Month': date_sequence.month,
            'Day': date_sequence.day,
            'MonthName': date_sequence.strftime('%B'),
            'DayOfWeek': date_sequence.strftime('%A'),
            'WeekendFlag': (date_sequence.weekday >= 5).astype(int)
        })

        print(f" Loading {len(date_dimension):,} date entries...")

        cursor = self.warehouse_connection.cursor()
        cursor.fast_executemany = True

        insertion_data = [
            (
                row.DateKey,
                row.Date,
                row.Year,
                row.Quarter,
                row.Month,
                row.Day,
                row.MonthName,
                row.DayOfWeek,
                row.WeekendFlag
            )
            for row in date_dimension.itertuples()
        ]

        cursor.executemany("""
            INSERT INTO DimDate 
            (DateKey, Date, Year, Quarter, Month, Day, MonthName, DayOfWeek, IsWeekend)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, insertion_data)

        self.warehouse_connection.commit()
        cursor.close()

        print(f" Date dimension populated: {len(date_dimension):,} entries")
        return date_dimension

    def build_legacy_system_mapping(self):
        """Construct mapping between legacy system IDs and business entities"""
        print("\n🗺️  LEGACY SYSTEM MAPPING CONSTRUCTION")
        print("-" * 30)

        entity_mapping = {
            'customer_mapping': {},  # Customer identifier to organization name
            'employee_mapping': {}   # Employee identifier to personnel name
        }

        try:
            legacy_connection_string = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            legacy_connection = pyodbc.connect(legacy_connection_string)

            # Map customer entities
            customer_records = pd.read_sql("SELECT [ID], [Company] FROM [Customers]", legacy_connection)
            for _, record in customer_records.iterrows():
                customer_identifier = str(record['ID'])
                organization_name = str(record['Company'])
                entity_mapping['customer_mapping'][customer_identifier] = organization_name

            # Map employee entities
            employee_records = pd.read_sql("SELECT [ID], [First Name], [Last Name] FROM [Employees]", legacy_connection)
            for _, record in employee_records.iterrows():
                employee_identifier = str(record['ID'])
                given_name = str(record['First Name'])
                family_name = str(record['Last Name'])
                complete_name = f"{given_name} {family_name}"
                entity_mapping['employee_mapping'][employee_identifier] = complete_name

            legacy_connection.close()

            print(f"  ✅ Mapping constructed: {len(entity_mapping['customer_mapping'])} customers, {len(entity_mapping['employee_mapping'])} employees")

        except Exception as e:
            print(f"  ❌ Mapping construction error: {e}")

        return entity_mapping

    def _verify_customer_dimension_structure(self):
        """Validate customer dimension table structure"""
        try:
            cursor = self.warehouse_connection.cursor()
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
            self.warehouse_connection.commit()
            cursor.close()
        except Exception as e:
            print(f"⚠️  Customer dimension structure issue: {e}")

    def _verify_employee_dimension_structure(self):
        """Validate employee dimension table structure"""
        try:
            cursor = self.warehouse_connection.cursor()
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
            self.warehouse_connection.commit()
            cursor.close()
        except Exception as e:
            print(f"⚠️  Employee dimension structure issue: {e}")

    def _verify_order_facts_structure(self):
        """Validate order facts table structure"""
        try:
            cursor = self.warehouse_connection.cursor()
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
                        DeliveryStatus BIT,
                        DeliveryDelay INT,
                        SourceSystem VARCHAR(20),
                        FOREIGN KEY (CustomerKey) REFERENCES DimCustomer(CustomerKey),
                        FOREIGN KEY (EmployeeKey) REFERENCES DimEmployee(EmployeeKey),
                        FOREIGN KEY (OrderDateKey) REFERENCES DimDate(DateKey)
                    );

                    -- Performance optimization indexes
                    CREATE INDEX IX_OrderDate_Reference ON FactOrders(OrderDateKey);
                    CREATE INDEX IX_Customer_Reference ON FactOrders(CustomerKey);
                    CREATE INDEX IX_Employee_Reference ON FactOrders(EmployeeKey);
                END
            """)
            self.warehouse_connection.commit()
            cursor.close()
            print("  ✅ Order facts structure verified")
        except Exception as e:
            print(f"  ❌ Order facts structure error: {e}")

    def _verify_date_dimension_structure(self):
        """Validate date dimension table structure"""
        try:
            cursor = self.warehouse_connection.cursor()
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
                        MonthName VARCHAR(20) NOT NULL,
                        DayOfWeek VARCHAR(20) NOT NULL,
                        IsWeekend BIT NOT NULL
                    );

                    -- Temporal query optimization
                    CREATE INDEX IX_Temporal_Date ON DimDate(Date);
                    CREATE INDEX IX_Temporal_Year ON DimDate(Year);
                    CREATE INDEX IX_Temporal_YearMonth ON DimDate(Year, Month);
                END
            """)
            self.warehouse_connection.commit()
            cursor.close()
            print("  ✅ Date dimension structure verified")
        except Exception as e:
            print(f"  ❌ Date dimension structure error: {e}")

    def prepare_reporting_dataset(self):
        """Compile comprehensive dataset for analytical reporting"""
        print("\n📊 ANALYTICAL DATASET PREPARATION")
        print("-" * 30)

        if self.warehouse_connection is None:
            print("  ❌ Data warehouse connection unavailable")
            return pd.DataFrame()

        try:
            analytical_query = """
            SELECT 
                fo.OrderID,
                fo.OrderDate,
                fo.RequiredDate,
                fo.ShippedDate,
                fo.Freight,
                fo.TotalAmount,
                fo.DeliveryStatus,
                fo.SourceSystem,
                fo.DeliveryDelay,
                dc.CustomerID,
                dc.CompanyName as CustomerOrganization,
                dc.Country as CustomerLocation,
                de.EmployeeID,
                de.FirstName + ' ' + de.LastName as EmployeeFullName,
                de.Title as EmployeePosition,
                dd.Year,
                dd.Month,
                dd.MonthName
            FROM FactOrders fo
            LEFT JOIN DimCustomer dc ON fo.CustomerKey = dc.CustomerKey
            LEFT JOIN DimEmployee de ON fo.EmployeeKey = de.EmployeeKey
            LEFT JOIN DimDate dd ON fo.OrderDateKey = dd.DateKey
            ORDER BY fo.OrderDate DESC
            """

            analytical_data = pd.read_sql(analytical_query, self.warehouse_connection)

            analytical_data['DeliveryStatusText'] = analytical_data['DeliveryStatus'].apply(lambda x: 'Completed' if x == 1 else 'Pending')

            if 'DeliveryDelay' in analytical_data.columns:
                analytical_data['DeliveryTimeliness'] = analytical_data['DeliveryDelay'].apply(
                    lambda x: 'On Schedule' if x <= 0 else ('Delayed' if x > 0 else 'Unknown')
                )

            print(f"  ✅ Analytical data compiled: {len(analytical_data)} records")

            analytical_data.to_csv('data/analytical_dataset.csv', index=False)
            print("  💾 Dataset archived to data/analytical_dataset.csv")

            return analytical_data

        except Exception as e:
            print(f"  ❌ Analytical dataset compilation error: {e}")
            return pd.DataFrame()


    # DATA ACQUISITION METHODS
    def acquire_operational_data(self):
        print("\n📥 OPERATIONAL DATA ACQUISITION")
        print("-" * 30)

        data_queries = {
            'customer_data': """
                SELECT CustomerID, CompanyName, ContactName, ContactTitle, 
                       Address, City, Region, PostalCode, Country, Phone
                FROM Customers
                WHERE CustomerID IS NOT NULL
            """,
            'employee_data': """
                SELECT EmployeeID, LastName, FirstName, Title, TitleOfCourtesy,
                       BirthDate, HireDate, Address, City, Region, PostalCode,
                       Country, HomePhone, ReportsTo
                FROM Employees
                WHERE EmployeeID IS NOT NULL
            """,
            'order_data': """
                SELECT o.OrderID, o.CustomerID, o.EmployeeID, 
                       o.OrderDate, o.RequiredDate, o.ShippedDate,
                       o.ShipVia, o.Freight, o.ShipName, o.ShipAddress,
                       o.ShipCity, o.ShipRegion, o.ShipPostalCode, o.ShipCountry,
                       SUM(od.Quantity * od.UnitPrice * (1 - od.Discount)) as TransactionValue
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

        acquired_data = {}
        for dataset_name, query in data_queries.items():
            try:
                acquired_data[dataset_name] = pd.read_sql(query, self.source_connection)
                print(f"  ✅ {dataset_name}: {len(acquired_data[dataset_name])} records acquired")
            except Exception as e:
                print(f"  ❌ Acquisition error for {dataset_name}: {e}")
                acquired_data[dataset_name] = pd.DataFrame()

        return acquired_data

    def acquire_legacy_system_data(self):
        """Extract raw data from legacy system without transformation"""
        if not DatabaseConfig.ACCESS_DB_PATH:
            print("\nℹ️  Legacy system path not configured")
            return {}

        print("\n📥 LEGACY SYSTEM DATA EXTRACTION (RAW)")
        print("-" * 30)

        try:
            legacy_connection_string = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={DatabaseConfig.ACCESS_DB_PATH};"
            legacy_connection = pyodbc.connect(legacy_connection_string)

            cursor = legacy_connection.cursor()
            available_tables = cursor.tables(tableType='TABLE')
            table_catalog = []
            for table in available_tables:
                table_catalog.append(table.table_name)
            cursor.close()

            print(f"Legacy system tables: {table_catalog}")

            raw_extraction = {}

            try:
                print("  Extracting customer data (raw)...")
                customer_source = 'Customers'
                if customer_source not in table_catalog:
                    for table in table_catalog:
                        if 'customer' in table.lower():
                            customer_source = table
                            break

                query = f"SELECT * FROM [{customer_source}]"
                raw_extraction['customer_raw'] = pd.read_sql(query, legacy_connection)
                print(f"  ✅ Raw customer data: {len(raw_extraction['customer_raw'])} records")
                print(f"    Attributes: {list(raw_extraction['customer_raw'].columns)}")
            except Exception as e:
                print(f"  ❌ Customer extraction error: {e}")
                raw_extraction['customer_raw'] = pd.DataFrame()

            try:
                print("  Extracting employee data (raw)...")
                employee_source = 'Employees'
                if employee_source not in table_catalog:
                    for table in table_catalog:
                        if 'employee' in table.lower():
                            employee_source = table
                            break

                query = f"SELECT * FROM [{employee_source}]"
                raw_extraction['employee_raw'] = pd.read_sql(query, legacy_connection)
                print(f"  ✅ Raw employee data: {len(raw_extraction['employee_raw'])} records")
            except Exception as e:
                print(f"  ❌ Employee extraction error: {e}")
                raw_extraction['employee_raw'] = pd.DataFrame()

            try:
                print("  Extracting order data (raw)...")
                order_source = 'Orders'
                if order_source not in table_catalog:
                    for table in table_catalog:
                        if 'order' in table.lower() and 'detail' not in table.lower():
                            order_source = table
                            break

                query = f"SELECT * FROM [{order_source}]"
                raw_extraction['order_raw'] = pd.read_sql(query, legacy_connection)
                print(f"  ✅ Raw order data: {len(raw_extraction['order_raw'])} records")
            except Exception as e:
                print(f"  ❌ Order extraction error: {e}")
                raw_extraction['order_raw'] = pd.DataFrame()

            try:
                print("  Extracting order detail data (raw)...")
                detail_source = 'Order Details'
                if detail_source not in table_catalog:
                    for table in table_catalog:
                        if 'order detail' in table.lower() or 'order_details' in table.lower():
                            detail_source = table
                            break

                query = f"SELECT * FROM [{detail_source}]"
                raw_extraction['order_detail_raw'] = pd.read_sql(query, legacy_connection)
                print(f"  ✅ Raw order detail data: {len(raw_extraction['order_detail_raw'])} records")
            except Exception as e:
                print(f"  ⚠️  Order detail extraction error: {e}")
                raw_extraction['order_detail_raw'] = pd.DataFrame()

            legacy_connection.close()

            data_present = False
            for key, dataset in raw_extraction.items():
                if not dataset.empty:
                    data_present = True
                    break

            if not data_present:
                print("  ℹ️  No data extracted from legacy system")

            return raw_extraction

        except Exception as e:
            print(f"  ❌ Legacy system access error: {e}")
            return {}


    # DATA TRANSFORMATION METHODS
    def process_customer_dimension(self, customer_dataset, source_identifier='SQL'):
        print(f"\n👥 CUSTOMER DIMENSION PROCESSING ({source_identifier})")
        print("-" * 30)

        if customer_dataset.empty:
            print("  ⚠️  Customer dataset empty")
            return pd.DataFrame()

        processed_customers = customer_dataset.copy()

        if source_identifier == 'Access':
            attribute_mapping = {
                'ID': 'CustomerID',
                'Company': 'CompanyName',
                'Last Name': 'LastName',
                'First Name': 'FirstName',
                'Business Phone': 'Phone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }

            for legacy_attribute, standard_attribute in attribute_mapping.items():
                if legacy_attribute in processed_customers.columns:
                    processed_customers = processed_customers.rename(columns={legacy_attribute: standard_attribute})

            if 'FirstName' in processed_customers.columns and 'LastName' in processed_customers.columns:
                processed_customers['ContactName'] = processed_customers['FirstName'].fillna('') + ' ' + processed_customers['LastName'].fillna('')
                processed_customers['ContactName'] = processed_customers['ContactName'].str.strip()

            if 'ContactTitle' not in processed_customers.columns:
                processed_customers['ContactTitle'] = 'Customer'

        else:
            attribute_mapping = {
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

        for legacy_attribute, standard_attribute in attribute_mapping.items():
            if legacy_attribute in processed_customers.columns and standard_attribute not in processed_customers.columns:
                processed_customers = processed_customers.rename(columns={legacy_attribute: standard_attribute})

        required_attributes = [
            'CustomerID', 'CompanyName', 'ContactName', 'ContactTitle',
            'Address', 'City', 'Region', 'PostalCode', 'Country', 'Phone'
        ]

        for attribute in required_attributes:
            if attribute not in processed_customers.columns:
                processed_customers[attribute] = None

        processed_customers['SourceSystem'] = source_identifier

        if source_identifier == 'Access':
            if 'CustomerID' in processed_customers.columns:
                processed_customers['CustomerID'] = pd.to_numeric(processed_customers['CustomerID'], errors='coerce')
                processed_customers['CustomerID'] = 'LEG-' + processed_customers['CustomerID'].fillna(0).astype(int).astype(str)

        if 'CustomerID' in processed_customers.columns:
            initial_records = len(processed_customers)
            processed_customers = processed_customers[processed_customers['CustomerID'].notna()]
            filtered_records = len(processed_customers)
            if filtered_records < initial_records:
                print(f"  ⚠️  {initial_records - filtered_records} records filtered (missing CustomerID)")

        if 'Region' in processed_customers.columns:
            processed_customers['Region'] = processed_customers['Region'].fillna('Unknown')
        if 'PostalCode' in processed_customers.columns:
            processed_customers['PostalCode'] = processed_customers['PostalCode'].fillna('Unknown')
        if 'ContactTitle' in processed_customers.columns:
            processed_customers['ContactTitle'] = processed_customers['ContactTitle'].fillna('Unknown')

        for attribute in processed_customers.columns:
            if processed_customers[attribute].dtype == 'object':
                processed_customers[attribute] = processed_customers[attribute].astype(str)

        available_attributes = [attr for attr in required_attributes if attr in processed_customers.columns]
        if available_attributes:
            processed_customers = processed_customers[available_attributes + ['SourceSystem']]

        print(f"  ✅ {len(processed_customers)} customers processed")
        return processed_customers

    def process_employee_dimension(self, employee_dataset, source_identifier='SQL'):
        print(f"\n👨‍💼 EMPLOYEE DIMENSION PROCESSING ({source_identifier})")
        print("-" * 30)

        if employee_dataset.empty:
            print("  ⚠️  Employee dataset empty")
            return pd.DataFrame()

        processed_employees = employee_dataset.copy()

        if source_identifier == 'Access':
            attribute_mapping = {
                'ID': 'EmployeeID',
                'Last Name': 'LastName',
                'First Name': 'FirstName',
                'Job Title': 'Title',
                'Business Phone': 'HomePhone',
                'Address': 'Address',
                'City': 'City',
                'State/Province': 'Region',
                'ZIP/Postal Code': 'PostalCode',
                'Country/Region': 'Country'
            }

            for legacy_attribute, standard_attribute in attribute_mapping.items():
                if legacy_attribute in processed_employees.columns:
                    processed_employees = processed_employees.rename(columns={legacy_attribute: standard_attribute})

            if 'TitleOfCourtesy' not in processed_employees.columns:
                processed_employees['TitleOfCourtesy'] = 'Mr.'
            if 'BirthDate' not in processed_employees.columns:
                processed_employees['BirthDate'] = None
            if 'HireDate' not in processed_employees.columns:
                processed_employees['HireDate'] = None
            if 'ReportsTo' not in processed_employees.columns:
                processed_employees['ReportsTo'] = None

        else:
            attribute_mapping = {
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

        for legacy_attribute, standard_attribute in attribute_mapping.items():
            if legacy_attribute in processed_employees.columns and standard_attribute not in processed_employees.columns:
                processed_employees = processed_employees.rename(columns={legacy_attribute: standard_attribute})

        required_attributes = [
            'EmployeeID', 'LastName', 'FirstName', 'Title', 'TitleOfCourtesy',
            'BirthDate', 'HireDate', 'Address', 'City', 'Region', 'PostalCode',
            'Country', 'HomePhone', 'ReportsTo'
        ]

        for attribute in required_attributes:
            if attribute not in processed_employees.columns:
                processed_employees[attribute] = None

        processed_employees['SourceSystem'] = source_identifier

        if source_identifier == 'Access':
            if 'EmployeeID' in processed_employees.columns:
                processed_employees['EmployeeID'] = pd.to_numeric(processed_employees['EmployeeID'], errors='coerce')
                processed_employees['EmployeeID'] = 2000 + processed_employees['EmployeeID'].fillna(0).astype(int)

        if 'EmployeeID' in processed_employees.columns:
            initial_records = len(processed_employees)
            processed_employees = processed_employees[processed_employees['EmployeeID'].notna()]
            filtered_records = len(processed_employees)
            if filtered_records < initial_records:
                print(f"  ⚠️  {initial_records - filtered_records} records filtered (missing EmployeeID)")

        temporal_attributes = ['BirthDate', 'HireDate']
        for attribute in temporal_attributes:
            if attribute in processed_employees.columns:
                processed_employees[attribute] = pd.to_datetime(processed_employees[attribute], errors='coerce')

        if 'Region' in processed_employees.columns:
            processed_employees['Region'] = processed_employees['Region'].fillna('Unknown')
        if 'PostalCode' in processed_employees.columns:
            processed_employees['PostalCode'] = processed_employees['PostalCode'].fillna('Unknown')
        if 'Title' in processed_employees.columns:
            processed_employees['Title'] = processed_employees['Title'].fillna('Unknown')
        if 'TitleOfCourtesy' in processed_employees.columns:
            processed_employees['TitleOfCourtesy'] = processed_employees['TitleOfCourtesy'].fillna('Unknown')

        if 'EmployeeID' in processed_employees.columns:
            processed_employees['EmployeeID'] = pd.to_numeric(processed_employees['EmployeeID'], errors='coerce')
            processed_employees = processed_employees[processed_employees['EmployeeID'].notna()]

        if 'ReportsTo' in processed_employees.columns:
            processed_employees['ReportsTo'] = pd.to_numeric(processed_employees['ReportsTo'], errors='coerce')

        for attribute in processed_employees.columns:
            if processed_employees[attribute].dtype == 'object':
                processed_employees[attribute] = processed_employees[attribute].astype(str)

        available_attributes = [attr for attr in required_attributes if attr in processed_employees.columns]
        if available_attributes:
            processed_employees = processed_employees[available_attributes + ['SourceSystem']]

        print(f"  ✅ {len(processed_employees)} employees processed")
        return processed_employees

    def process_order_facts(self, order_dataset, source_identifier='SQL'):
        print(f"\n📦 ORDER FACTS PROCESSING ({source_identifier})")
        print("-" * 30)

        if order_dataset.empty:
            print("  ⚠️  Order dataset empty")
            return pd.DataFrame()

        processed_orders = order_dataset.copy()

        if source_identifier == 'Access' and 'TransactionValue' not in processed_orders.columns:
            print("  ℹ️  Calculating transaction values from order details...")

        if source_identifier == 'Access':
            attribute_mapping = {
                'Order ID': 'OrderID',
                'ID': 'OrderID',
                'Customer': 'CustomerID',
                'Employee': 'EmployeeID',
                'Order Date': 'OrderDate',
                'Required Date': 'RequiredDate',
                'Shipped Date': 'ShippedDate',
                'Shipping Fee': 'Freight',
                'Ship Fee': 'Freight',
                'Ship Name': 'ShipName',
                'Ship Address': 'ShipAddress',
                'Ship City': 'ShipCity',
                'Ship State/Province': 'ShipRegion',
                'Ship Region': 'ShipRegion',
                'Ship ZIP/Postal Code': 'ShipPostalCode',
                'Ship Postal Code': 'ShipPostalCode',
                'Ship Country/Region': 'ShipCountry',
                'Ship Country': 'ShipCountry'
            }

            for legacy_attribute, standard_attribute in attribute_mapping.items():
                if legacy_attribute in processed_orders.columns:
                    processed_orders = processed_orders.rename(columns={legacy_attribute: standard_attribute})

            if 'ShipVia' not in processed_orders.columns:
                processed_orders['ShipVia'] = 1
            if 'TransactionValue' not in processed_orders.columns:
                processed_orders['TransactionValue'] = 0.0

        else:
            attribute_mapping = {
                'OrderID': 'OrderID',
                'CustomerID': 'CustomerID',
                'EmployeeID': 'EmployeeID',
                'OrderDate': 'OrderDate',
                'RequiredDate': 'RequiredDate',
                'ShippedDate': 'ShippedDate',
                'ShipVia': 'ShipVia',
                'Freight': 'Freight',
                'ShipName': 'ShipName',
                'ShipAddress': 'ShipAddress',
                'ShipCity': 'ShipCity',
                'ShipRegion': 'ShipRegion',
                'ShipPostalCode': 'ShipPostalCode',
                'ShipCountry': 'ShipCountry',
                'TransactionValue': 'TransactionValue'
            }

        for legacy_attribute, standard_attribute in attribute_mapping.items():
            if legacy_attribute in processed_orders.columns and standard_attribute not in processed_orders.columns:
                processed_orders = processed_orders.rename(columns={legacy_attribute: standard_attribute})

        required_attributes = [
            'OrderID', 'CustomerID', 'EmployeeID', 'OrderDate',
            'RequiredDate', 'ShippedDate', 'ShipVia', 'Freight',
            'ShipName', 'ShipAddress', 'ShipCity', 'ShipRegion',
            'ShipPostalCode', 'ShipCountry', 'TransactionValue'
        ]

        for attribute in required_attributes:
            if attribute not in processed_orders.columns:
                processed_orders[attribute] = None

        temporal_attributes = ['OrderDate', 'RequiredDate', 'ShippedDate']
        for attribute in temporal_attributes:
            if attribute in processed_orders.columns:
                processed_orders[attribute] = pd.to_datetime(processed_orders[attribute], errors='coerce')

        processed_orders['DeliveryStatus'] = processed_orders['ShippedDate'].notna().astype(int)

        if 'ShippedDate' in processed_orders.columns and 'RequiredDate' in processed_orders.columns:
            processed_orders['DeliveryDelay'] = np.where(
                processed_orders['ShippedDate'].notna() & processed_orders['RequiredDate'].notna(),
                (processed_orders['ShippedDate'] - processed_orders['RequiredDate']).dt.days,
                None
            )
        else:
            processed_orders['DeliveryDelay'] = None

        processed_orders['SourceSystem'] = source_identifier

        if source_identifier == 'Access':
            if 'CustomerID' in processed_orders.columns:
                try:
                    processed_orders['CustomerID'] = pd.to_numeric(processed_orders['CustomerID'], errors='coerce')

                    invalid_customers = processed_orders['CustomerID'].isna() | (processed_orders['CustomerID'] <= 0)
                    valid_customers = ~invalid_customers

                    if invalid_customers.any():
                        print(f"  ⚠️  Found {invalid_customers.sum()} legacy orders with invalid CustomerID")
                        processed_orders.loc[invalid_customers, 'CustomerID'] = None

                    if valid_customers.any():
                        processed_orders.loc[valid_customers, 'CustomerID'] = 'LEG-' + processed_orders.loc[
                            valid_customers, 'CustomerID'].astype(int).astype(str)

                except Exception as e:
                    print(f"  ⚠️  CustomerID processing issue: {e}")
                    pass

            if 'EmployeeID' in processed_orders.columns:
                try:
                    processed_orders['EmployeeID'] = pd.to_numeric(processed_orders['EmployeeID'], errors='coerce')

                    invalid_employees = processed_orders['EmployeeID'].isna() | (processed_orders['EmployeeID'] <= 0)
                    valid_employees = ~invalid_employees

                    if invalid_employees.any():
                        print(f"  ⚠️  Found {invalid_employees.sum()} legacy orders with invalid EmployeeID")
                        processed_orders.loc[invalid_employees, 'EmployeeID'] = None

                    if valid_employees.any():
                        processed_orders.loc[valid_employees, 'EmployeeID'] = 2000 + processed_orders.loc[
                            valid_employees, 'EmployeeID'].astype(int)

                except Exception as e:
                    print(f"  ⚠️  EmployeeID processing issue: {e}")
                    pass

            if 'CustomerID' in processed_orders.columns and 'EmployeeID' in processed_orders.columns:
                issue_records = processed_orders[
                    (processed_orders['CustomerID'].isna()) |
                    (processed_orders['EmployeeID'].isna()) |
                    (processed_orders['CustomerID'] == 'LEG-0') |
                    (processed_orders['EmployeeID'] == 2000)
                    ]

                if not issue_records.empty:
                    print(f"  ⚠️  Sample problematic records:")
                    for idx, record in issue_records.head().iterrows():
                        print(f"    Order {record.get('OrderID', 'N/A')}: "
                              f"Customer={record.get('CustomerID', 'N/A')}, "
                              f"Employee={record.get('EmployeeID', 'N/A')}")

        if 'Freight' in processed_orders.columns:
            processed_orders['Freight'] = pd.to_numeric(processed_orders['Freight'], errors='coerce').fillna(0)
        if 'TransactionValue' in processed_orders.columns:
            processed_orders['TransactionValue'] = pd.to_numeric(processed_orders['TransactionValue'], errors='coerce').fillna(0)
        if 'ShipVia' in processed_orders.columns:
            processed_orders['ShipVia'] = pd.to_numeric(processed_orders['ShipVia'], errors='coerce').fillna(1)

        for attribute in processed_orders.columns:
            if processed_orders[attribute].dtype == 'object':
                processed_orders[attribute] = processed_orders[attribute].astype(str)

        retention_attributes = required_attributes + ['DeliveryStatus', 'DeliveryDelay', 'SourceSystem']
        available_attributes = [attr for attr in retention_attributes if attr in processed_orders.columns]
        if available_attributes:
            processed_orders = processed_orders[available_attributes]

        print(f"  ✅ {len(processed_orders)} orders processed")

        if source_identifier == 'Access':
            incomplete_records = processed_orders[
                (processed_orders['CustomerID'].isna()) |
                (processed_orders['EmployeeID'].isna())
                ].shape[0]
            if incomplete_records > 0:
                print(f"  ℹ️  {incomplete_records} legacy orders have incomplete references")

        return processed_orders


    # DATA LOADING METHODS
    def load_dimension_tables(self, customer_dimension, employee_dimension):
        print("\n📤 DIMENSION TABLE POPULATION")
        print("-" * 30)

        if self.warehouse_connection is None:
            print("  ❌ Warehouse connection unavailable")
            return

        self._verify_customer_dimension_structure()
        self._verify_employee_dimension_structure()

        if not customer_dimension.empty:
            print("  📋 Populating customer dimension...")
            try:
                existing_customer_query = "SELECT CustomerID, SourceSystem FROM DimCustomer"
                existing_customers = pd.read_sql(existing_customer_query, self.warehouse_connection)

                if not existing_customers.empty:
                    customer_dimension['composite_identifier'] = customer_dimension['CustomerID'].astype(str) + '_' + customer_dimension['SourceSystem'].astype(str)
                    existing_customers['composite_identifier'] = existing_customers['CustomerID'].astype(str) + '_' + existing_customers['SourceSystem'].astype(str)

                    new_customers = customer_dimension[
                        ~customer_dimension['composite_identifier'].isin(existing_customers['composite_identifier'])]
                    customer_dimension = new_customers.drop('composite_identifier', axis=1, errors='ignore')

                if not customer_dimension.empty:
                    cursor = self.warehouse_connection.cursor()
                    insertion_count = 0

                    for _, record in customer_dimension.iterrows():
                        try:
                            customer_identifier = str(record.get('CustomerID', '')) if pd.notna(record.get('CustomerID')) else ''

                            if not customer_identifier:
                                continue

                            cursor.execute("""
                                INSERT INTO DimCustomer (CustomerID, CompanyName, ContactName, ContactTitle, 
                                Address, City, Region, PostalCode, Country, Phone, SourceSystem)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                           customer_identifier,
                                           str(record.get('CompanyName', '')) if pd.notna(record.get('CompanyName')) else '',
                                           str(record.get('ContactName', '')) if pd.notna(record.get('ContactName')) else '',
                                           str(record.get('ContactTitle', '')) if pd.notna(record.get('ContactTitle')) else '',
                                           str(record.get('Address', '')) if pd.notna(record.get('Address')) else '',
                                           str(record.get('City', '')) if pd.notna(record.get('City')) else '',
                                           str(record.get('Region', '')) if pd.notna(record.get('Region')) else '',
                                           str(record.get('PostalCode', '')) if pd.notna(record.get('PostalCode')) else '',
                                           str(record.get('Country', '')) if pd.notna(record.get('Country')) else '',
                                           str(record.get('Phone', '')) if pd.notna(record.get('Phone')) else '',
                                           str(record.get('SourceSystem', 'Unknown')) if pd.notna(record.get('SourceSystem')) else 'Unknown')

                            insertion_count += 1

                        except Exception as record_error:
                            print(f"    ⚠️  Record error {_}: {record_error}")
                            continue

                    self.warehouse_connection.commit()
                    cursor.close()

                    print(f"    ✅ {insertion_count} new customers added")
                else:
                    print("    ℹ️  All customer records already exist")

            except Exception as e:
                print(f"    ❌ Customer dimension population error: {e}")
        else:
            print("  ℹ️  No customer data to load")

        if not employee_dimension.empty:
            print("  📋 Populating employee dimension...")
            try:
                existing_employee_query = "SELECT EmployeeID, SourceSystem FROM DimEmployee"
                existing_employees = pd.read_sql(existing_employee_query, self.warehouse_connection)

                if not existing_employees.empty:
                    employee_dimension['composite_identifier'] = employee_dimension['EmployeeID'].astype(str) + '_' + employee_dimension['SourceSystem'].astype(str)
                    existing_employees['composite_identifier'] = existing_employees['EmployeeID'].astype(str) + '_' + existing_employees['SourceSystem'].astype(str)

                    new_employees = employee_dimension[
                        ~employee_dimension['composite_identifier'].isin(existing_employees['composite_identifier'])]
                    employee_dimension = new_employees.drop('composite_identifier', axis=1, errors='ignore')

                if not employee_dimension.empty:
                    cursor = self.warehouse_connection.cursor()
                    insertion_count = 0

                    for _, record in employee_dimension.iterrows():
                        try:
                            employee_identifier = int(record.get('EmployeeID', 0)) if pd.notna(record.get('EmployeeID')) else 0

                            if employee_identifier == 0:
                                continue

                            supervisor_reference = record.get('ReportsTo')
                            if pd.isna(supervisor_reference):
                                supervisor_reference = None
                            else:
                                supervisor_reference = int(supervisor_reference)

                            cursor.execute("""
                                INSERT INTO DimEmployee (EmployeeID, LastName, FirstName, Title, 
                                TitleOfCourtesy, BirthDate, HireDate, Address, City, Region, 
                                PostalCode, Country, HomePhone, ReportsTo, SourceSystem)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                                           employee_identifier,
                                           str(record.get('LastName', '')) if pd.notna(record.get('LastName')) else '',
                                           str(record.get('FirstName', '')) if pd.notna(record.get('FirstName')) else '',
                                           str(record.get('Title', '')) if pd.notna(record.get('Title')) else '',
                                           str(record.get('TitleOfCourtesy', '')) if pd.notna(record.get('TitleOfCourtesy')) else '',
                                           record.get('BirthDate') if pd.notna(record.get('BirthDate')) else None,
                                           record.get('HireDate') if pd.notna(record.get('HireDate')) else None,
                                           str(record.get('Address', '')) if pd.notna(record.get('Address')) else '',
                                           str(record.get('City', '')) if pd.notna(record.get('City')) else '',
                                           str(record.get('Region', '')) if pd.notna(record.get('Region')) else '',
                                           str(record.get('PostalCode', '')) if pd.notna(record.get('PostalCode')) else '',
                                           str(record.get('Country', '')) if pd.notna(record.get('Country')) else '',
                                           str(record.get('HomePhone', '')) if pd.notna(record.get('HomePhone')) else '',
                                           supervisor_reference,
                                           str(record.get('SourceSystem', 'Unknown')) if pd.notna(record.get('SourceSystem')) else 'Unknown')

                            insertion_count += 1

                        except Exception as record_error:
                            print(f"    ⚠️  Record error {_}: {record_error}")
                            continue

                    self.warehouse_connection.commit()
                    cursor.close()

                    print(f"    ✅ {insertion_count} new employees added")
                else:
                    print("    ℹ️  All employee records already exist")

            except Exception as e:
                print(f"    ❌ Employee dimension population error: {e}")
        else:
            print("  ℹ️  No employee data to load")

    def load_fact_tables(self, order_facts):
        print("\n📤 FACT TABLE POPULATION")
        print("-" * 30)

        if self.warehouse_connection is None or order_facts.empty:
            print("  ℹ️  No fact data available")
            return

        legacy_mapping = self.build_legacy_system_mapping()

        self._verify_order_facts_structure()

        print("  🔍 Intelligent reference resolution...")

        try:
            cursor = self.warehouse_connection.cursor()

            existing_fact_query = "SELECT OrderID, SourceSystem FROM FactOrders"
            existing_facts = pd.read_sql(existing_fact_query, self.warehouse_connection)

            if not existing_facts.empty:
                order_facts['composite_identifier'] = order_facts['OrderID'].astype(str) + '_' + order_facts['SourceSystem'].astype(str)
                existing_facts['composite_identifier'] = existing_facts['OrderID'].astype(str) + '_' + existing_facts['SourceSystem'].astype(str)

                new_facts = order_facts[~order_facts['composite_identifier'].isin(existing_facts['composite_identifier'])]
                order_facts = new_facts.drop('composite_identifier', axis=1, errors='ignore')

            if order_facts.empty:
                print("  ℹ️  All fact records already exist")
                return

            prepared_facts = order_facts.copy()
            if 'OrderDate' in prepared_facts.columns:
                prepared_facts['OrderDate'] = pd.to_datetime(prepared_facts['OrderDate'], errors='coerce')
                prepared_facts['OrderDateKey'] = prepared_facts['OrderDate'].dt.strftime('%Y%m%d').astype('Int64')

            insertion_count = 0
            error_count = 0

            for idx, record in prepared_facts.iterrows():
                try:
                    source_system = record.get('SourceSystem', 'SQL')
                    order_identifier = int(record.get('OrderID', 0)) if pd.notna(record.get('OrderID')) else 0

                    if order_identifier == 0:
                        continue

                    order_date = record.get('OrderDate')
                    date_key = None
                    if pd.notna(order_date):
                        try:
                            date_key = int(pd.to_datetime(order_date).strftime('%Y%m%d'))
                        except:
                            pass

                    if date_key is None:
                        print(f"    ⚠️  Order {order_identifier} excluded: missing order date")
                        continue

                    customer_reference = None
                    customer_identifier = record.get('CustomerID')

                    if source_system == 'SQL':
                        if pd.notna(customer_identifier):
                            cursor.execute("""
                                SELECT TOP 1 CustomerKey FROM DimCustomer 
                                WHERE CustomerID = ? AND SourceSystem = 'SQL'
                            """, (str(customer_identifier),))
                            reference_result = cursor.fetchone()
                            if reference_result:
                                customer_reference = reference_result[0]
                            else:
                                print(f"    ⚠️  SQL customer {customer_identifier} not located")

                    elif source_system == 'Access':
                        if pd.notna(customer_identifier):
                            legacy_customer_identifier = f"LEG-{customer_identifier}"
                            cursor.execute("""
                                SELECT TOP 1 CustomerKey FROM DimCustomer 
                                WHERE CustomerID = ? AND SourceSystem = 'Access'
                            """, (legacy_customer_identifier,))
                            reference_result = cursor.fetchone()

                            if reference_result:
                                customer_reference = reference_result[0]
                            else:
                                organization_name = legacy_mapping['customer_mapping'].get(str(customer_identifier))
                                if organization_name:
                                    cursor.execute("""
                                        SELECT TOP 1 CustomerKey FROM DimCustomer 
                                        WHERE CompanyName LIKE ? AND SourceSystem = 'Access'
                                    """, (f"%{organization_name}%",))
                                    reference_result = cursor.fetchone()
                                    if reference_result:
                                        customer_reference = reference_result[0]
                                else:
                                    print(f"    ⚠️  Legacy customer {customer_identifier} not located")

                    employee_reference = None
                    employee_identifier = record.get('EmployeeID')

                    if source_system == 'SQL':
                        if pd.notna(employee_identifier):
                            try:
                                personnel_identifier = int(employee_identifier)
                                cursor.execute("""
                                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                    WHERE EmployeeID = ? AND SourceSystem = 'SQL'
                                """, (personnel_identifier,))
                                reference_result = cursor.fetchone()
                                if reference_result:
                                    employee_reference = reference_result[0]
                                else:
                                    print(f"    ⚠️  SQL employee {employee_identifier} not located")
                            except:
                                pass

                    elif source_system == 'Access':
                        if pd.notna(employee_identifier):
                            try:
                                legacy_employee_identifier = 2000 + int(employee_identifier)
                                cursor.execute("""
                                    SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                    WHERE EmployeeID = ? AND SourceSystem = 'Access'
                                """, (legacy_employee_identifier,))
                                reference_result = cursor.fetchone()

                                if reference_result:
                                    employee_reference = reference_result[0]
                                else:
                                    personnel_name = legacy_mapping['employee_mapping'].get(str(employee_identifier))
                                    if personnel_name:
                                        cursor.execute("""
                                            SELECT TOP 1 EmployeeKey FROM DimEmployee 
                                            WHERE (FirstName + ' ' + LastName LIKE ? 
                                                   OR LastName + ', ' + FirstName LIKE ?)
                                            AND SourceSystem = 'Access'
                                        """, (f"%{personnel_name}%", f"%{personnel_name}%"))
                                        reference_result = cursor.fetchone()
                                        if reference_result:
                                            employee_reference = reference_result[0]
                                    else:
                                        print(f"    ⚠️  Legacy employee {employee_identifier} not located")
                            except:
                                pass

                    if customer_reference is None:
                        print(f"    ℹ️  Order {order_identifier}: Customer reference missing (ID: {customer_identifier})")
                    if employee_reference is None:
                        print(f"    ℹ️  Order {order_identifier}: Employee reference missing (ID: {employee_identifier})")

                    cursor.execute("""
                        INSERT INTO FactOrders (
                            OrderID, CustomerKey, EmployeeKey, OrderDateKey,
                            OrderDate, ShippedDate, ShipVia, Freight,
                            ShipName, ShipAddress, ShipCity, ShipRegion,
                            ShipPostalCode, ShipCountry, TotalAmount,
                            DeliveryStatus, SourceSystem
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                                   order_identifier,
                                   customer_reference,
                                   employee_reference,
                                   date_key,
                                   order_date if pd.notna(order_date) else None,
                                   record.get('ShippedDate') if pd.notna(record.get('ShippedDate')) else None,
                                   int(record.get('ShipVia', 0)) if pd.notna(record.get('ShipVia')) else 0,
                                   float(record.get('Freight', 0)) if pd.notna(record.get('Freight')) else 0.0,
                                   str(record.get('ShipName', '')) if pd.notna(record.get('ShipName')) else '',
                                   str(record.get('ShipAddress', '')) if pd.notna(record.get('ShipAddress')) else '',
                                   str(record.get('ShipCity', '')) if pd.notna(record.get('ShipCity')) else '',
                                   str(record.get('ShipRegion', '')) if pd.notna(record.get('ShipRegion')) else '',
                                   str(record.get('ShipPostalCode', '')) if pd.notna(record.get('ShipPostalCode')) else '',
                                   str(record.get('ShipCountry', '')) if pd.notna(record.get('ShipCountry')) else '',
                                   float(record.get('TransactionValue', 0)) if pd.notna(record.get('TransactionValue')) else 0.0,
                                   int(record.get('DeliveryStatus', 0)) if pd.notna(record.get('DeliveryStatus')) else 0,
                                   str(source_system)
                                   )

                    insertion_count += 1

                    if insertion_count % 20 == 0:
                        print(f"    {insertion_count} fact records inserted...")

                except Exception as record_error:
                    error_count += 1
                    if error_count <= 10:
                        print(f"    ⚠️  Record error {idx}: {str(record_error)[:80]}")
                    continue

            self.warehouse_connection.commit()
            cursor.close()

            print(f"\n  ✅ {insertion_count} fact records loaded")
            print(f"  ℹ️  Loading summary:")
            print(f"    - Records with customer reference: {insertion_count - error_count}")
            print(f"    - Records with employee reference: {insertion_count - error_count}")
            if error_count > 0:
                print(f"    - Loading errors: {error_count}")

        except Exception as e:
            print(f"  ❌ Fact loading error: {e}")
            import traceback
            traceback.print_exc()


    # SUMMARY REPORTING
    def generate_warehouse_summary(self):
        print("\n📊 DATA WAREHOUSE SUMMARY REPORT")
        print("-" * 30)

        if self.warehouse_connection is None:
            print("❌ Warehouse connection unavailable")
            return

        warehouse_tables = ['DimDate', 'DimCustomer', 'DimEmployee', 'FactOrders']
        for table in warehouse_tables:
            try:
                cursor = self.warehouse_connection.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                record_count = cursor.fetchone()[0]
                cursor.close()
                print(f"  {table}: {record_count} records")
            except Exception as e:
                print(f"  {table}: TABLE UNAVAILABLE")


    def execute_full_pipeline(self):
        print("\n" + "=" * 50)
        print("🚀 COMPLETE DATA INTEGRATION PIPELINE")
        print("=" * 50)

        try:
            self._verify_date_dimension_structure()
            self._verify_customer_dimension_structure()
            self._verify_employee_dimension_structure()
            self._verify_order_facts_structure()

            self.populate_date_dimension(1990, 2025)

            operational_data = self.acquire_operational_data()

            processed_customers_sql = self.process_customer_dimension(operational_data.get('customer_data', pd.DataFrame()), 'SQL')
            processed_employees_sql = self.process_employee_dimension(operational_data.get('employee_data', pd.DataFrame()), 'SQL')
            processed_orders_sql = self.process_order_facts(operational_data.get('order_data', pd.DataFrame()), 'SQL')

            legacy_data = self.acquire_legacy_system_data()

            if legacy_data:
                processed_customers_legacy = self.process_customer_dimension(
                    legacy_data.get('customer_raw', pd.DataFrame()), 'Access'
                )
                processed_employees_legacy = self.process_employee_dimension(
                    legacy_data.get('employee_raw', pd.DataFrame()), 'Access'
                )
                processed_orders_legacy = self.process_order_facts(
                    legacy_data.get('order_raw', pd.DataFrame()), 'Access'
                )

                consolidated_customers = pd.concat([processed_customers_sql, processed_customers_legacy], ignore_index=True)
                consolidated_employees = pd.concat([processed_employees_sql, processed_employees_legacy], ignore_index=True)
                consolidated_orders = pd.concat([processed_orders_sql, processed_orders_legacy], ignore_index=True)
            else:
                consolidated_customers = processed_customers_sql
                consolidated_employees = processed_employees_sql
                consolidated_orders = processed_orders_sql

            self.load_dimension_tables(consolidated_customers, consolidated_employees)
            self.load_fact_tables(consolidated_orders)

            print("\n🎯 ANALYTICAL DATA PREPARATION")
            print("-" * 30)

            try:
                import os
                if not os.path.exists('data'):
                    os.makedirs('data')
                consolidated_orders.to_csv('data/processed/consolidated_order_facts.csv', index=False)
                print("  ✅ Data archived to data/consolidated_order_facts.csv")
            except Exception as e:
                print(f"  ⚠️  Data archiving issue: {e}")

            self.generate_warehouse_summary()

            print("\n" + "=" * 50)
            print("🎉 DATA INTEGRATION COMPLETED SUCCESSFULLY!")
            print("=" * 50)

        except Exception as e:
            print(f"\n❌ PIPELINE EXECUTION ERROR: {e}")
            raise



# EXECUTION ENTRY POINT
if __name__ == "__main__":
    try:
        print("🚀 INITIATING DATA INTEGRATION PIPELINE")
        print("=" * 50)

        integration_pipeline = etl()
        integration_pipeline.execute_full_pipeline()

    except Exception as e:
        print(f"\n❌ EXECUTION TERMINATION: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n" + "=" * 50)
        print("🏁 PIPELINE EXECUTION COMPLETE")
        print("=" * 50)