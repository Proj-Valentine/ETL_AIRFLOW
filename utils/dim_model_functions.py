import duckdb

class DimModelFunctions:
    def __init__(self, db_connection):
        self.con = db_connection
    
    def create_raw_comp_data_table(self):
        sql = """
        CREATE OR REPLACE TABLE raw_comp_data AS
        SELECT *
        FROM read_csv_auto('SorensonworkfirmFIVEdata.csv')
        """
        self.con.sql(sql)
        
    # DATA EXPLORATION AND CLEANING
    
    def explore_raw_comp_data(self):
        dat = self.con.sql("SELECT * FROM raw_comp_data LIMIT 5").fetchall()
        for row in dat[:10]:
            print(row)
    
    def describe_raw_comp_data(self):
        meta_data = self.con.sql("DESCRIBE raw_comp_data").fetchall()
        meta_data_dict = {col[0]: col[1] for col in meta_data}
        print(meta_data_dict)
    
    def transform_raw_comp_data(self):
        sql = """
        ALTER TABLE raw_comp_data ALTER COLUMN zip SET DATA TYPE VARCHAR;
        UPDATE raw_comp_data SET zip = RIGHT('0' || zip, 5);
        UPDATE raw_comp_data SET lat = DEGREES(lat), lon = DEGREES(lon);
        """
        self.con.sql(sql)
        
    # CREATING SCHEMA
    
    def create_compustatistics_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.compustatistics (
            cusip_head_id VARCHAR,
            cusip_head_name VARCHAR,
            cusip_historical_id VARCHAR,
            cusip_historical_name VARCHAR,
            PRIMARY KEY (cusip_head_id)
        );
        """
    def create_ceo_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.ceos (
            ceo_id VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            PRIMARY KEY (ceo_id)
        );
        """
        self.con.sql(sql)
        
    def create_locations_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.locations (
            location_id VARCHAR,
            zip_code VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE,
            PRIMARY KEY (location_id)
        );"""
        self.con.sql(sql)
        
    def create_companies_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.companies (
            company_id VARCHAR,
            company_name VARCHAR,
            cusip_head_id VARCHAR REFERENCES {schema}.compustatistics (cusip_head_id),
            ceo_id VARCHAR REFERENCES {schema}.ceos (ceo_id),
            founding_year INTEGER,
            ownership_type VARCHAR,
            exit_type VARCHAR,
            exit_comment VARCHAR,
            location_id VARCHAR REFERENCES {schema}.locations (location_id),
            PRIMARY KEY (company_id)
        );
        """
        self.con.sql(sql)
        
    def create_product_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.product (
            product_id VARCHAR,
            product_types VARCHAR,
            PRIMARY KEY (product_id)
        );
        """
        self.con.sql(sql)
        
    def create_integrations_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.integrations (
            integration_id VARCHAR,
            cpu_source VARCHAR,
            os_source VARCHAR,
            application_source VARCHAR,
            communications_hardware_source VARCHAR,
            disk_source VARCHAR,
            ram_source VARCHAR,
            motherboard_source VARCHAR,
            PRIMARY KEY (integration_id)
        );
        """
        self.con.sql(sql)
        
    def create_years_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.years (
            year INTEGER,
            zodiac_type VARCHAR
        );
        """
        self.con.sql(sql)
        
    def create_workstation_sales_table(self, schema):
        sql = f"""
        CREATE OR REPLACE TABLE {schema}.workstation_sales (
            year_id INTEGER,
            company_id VARCHAR REFERENCES {schema}.companies (company_id),
            integration_id VARCHAR REFERENCES {schema}.integrations (integration_id),
            product_offering_id VARCHAR REFERENCES {schema}.product (product_id),
            sales DOUBLE,
            research_budget DOUBLE,
            employee_count DOUBLE,
            product_offering_count INTEGER,
            product_category_count INTEGER
        );
        """
        self.con.sql(sql)
        
    # LOADING METHODS
      
    def load_data_compustatistics(self, schema):
        sql = f"""
        INSERT INTO {schema}.compustatistics (cusip_head_id, cusip_head_name, cusip_historical_id, cusip_historical_name)
        WITH cte AS (
            SELECT DISTINCT
                CUSIPHEADERID AS cusip_head_id,
                CUSIPHEADERNAME AS cusip_head_name,
                CUSIPHISTORYID AS cusip_historical_id,
                CUSIPHISTORYNAME AS cusip_historical_name
            FROM
                raw_comp_data
        )
        SELECT
            MD5(COALESCE(cusip_head_id || cusip_head_name || cusip_historical_id || cusip_historical_name, '-1')) AS cusip_head_id,
            cusip_head_name,
            cusip_historical_id,
            cusip_historical_name
        FROM
            cte;
        """
        
        self.con.sql(sql)
    
    def load_ceos_table(self, schema):
        sql = f"""
        INSERT INTO {schema}.ceos (ceo_id, first_name, last_name)
        SELECT DISTINCT
            MD5(COALESCE(ceo, '-1')) AS ceo_id,
            SPLIT_PART(CEO, ' ', 1) AS first_name,
            SPLIT_PART(CEO, ' ', -1) AS last_name
        FROM
            raw_comp_data;
        """
        self.con.sql(sql)
    
    def load_locations_table(self, schema):
        sql = f"""
        INSERT INTO {schema}.locations (location_id, zip_code, latitude, longitude)
        SELECT DISTINCT
            MD5(COALESCE(zip || CAST(lat AS VARCHAR) || CAST(lon AS VARCHAR), '-1')),
            zip,
            lat,
            lon
        FROM
            raw_comp_data;
        """
        self.con.sql(sql)
    
    def load_companies_table(self, schema):
        sql = f"""
        INSERT INTO {schema}.companies (company_id, company_name, cusip_head_id, ceo_id, founding_year, ownership_type, exit_type, exit_comment, location_id)
        SELECT DISTINCT
            MD5(COALESCE(
                COALESCE(firmname, '-1') ||
                COALESCE(CAST(CUSIPHEADERID AS VARCHAR), '-1') ||
                COALESCE(CAST(firmid AS VARCHAR), '-1') ||
                COALESCE(CAST(found AS VARCHAR), '-1') ||
                COALESCE(CAST(own AS VARCHAR), '-1') ||
                COALESCE(CAST(fate AS VARCHAR), '-1') ||
                COALESCE(CAST(estate AS VARCHAR), '-1') ||
                COALESCE(ceo, '-1') ||
                COALESCE(zip, '-1')
            , '-1')) AS company_id,
            firmname as company_name,
            MD5(COALESCE(CUSIPHEADERID || CUSIPHEADERNAME || CUSIPHISTORYID || CUSIPHISTORYNAME, '-1')) as cusip_head_id,
            MD5(COALESCE(ceo, '-1')) as ceo_id,
            found as founding_year,
            CASE own
                WHEN 0 THEN 'Private'
                WHEN 1 THEN 'Public'
                WHEN 2 THEN 'Subsidiary'
                ELSE NULL
            END as ownership_type,
            CASE estate
                WHEN 0 THEN 'Censored'
                WHEN 1 THEN 'Exited market'
                WHEN 2 THEN 'Acquired'
                WHEN 3 THEN 'Spun off'
                WHEN 4 THEN 'Changed Name'
                ELSE NULL
            END as exit_type,
            fate as exit_comment,
            MD5(COALESCE(zip || CAST(lat AS VARCHAR) || CAST(lon AS VARCHAR), '-1')) as location_id
        FROM
            raw_comp_data;
        """
        self.con.sql(sql)
    
    def load_product_table(self, schema):
        sql = f"""
        INSERT INTO {schema}.product (product_id, product_types)
        WITH po AS (
            SELECT
                CASE
                    WHEN system = 1 AND graphic = 0 THEN 'Desktop systems'
                    WHEN system = 0 AND graphic = 1 THEN 'Graphics systems'
                    WHEN system = 1 AND graphic = 1 THEN 'Desktop and Graphics systems'
                    ELSE NULL
                END as offering
            from raw_comp_data
        )
        SELECT DISTINCT
            MD5(COALESCE(offering, '-1')),
            offering
        FROM
            po;
        """
        self.con.sql(sql)
    
    def load_integrations_table(self, schema):
        sql = f"""
        INSERT INTO {schema}.integrations
        (
            integration_id, 
            cpu_source, 
            os_source, 
            application_source, 
            communications_hardware_source, 
            disk_source, 
            ram_source, 
            motherboard_source
        )
        SELECT DISTINCT
            MD5(
                CASE WHEN cpu = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN os = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN apps = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN comm = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN disk = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN memory = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN board = 0 THEN 'Bought' ELSE 'Produced' END
            ), 
            CASE WHEN cpu = 0 THEN 'Bought' ELSE 'Produced' END,
            CASE WHEN os = 0 THEN 'Bought' ELSE 'Produced' END,
            CASE WHEN apps = 0 THEN 'Bought' ELSE 'Produced' END,
            CASE WHEN comm = 0 THEN 'Bought' ELSE 'Produced' END,
            CASE WHEN disk = 0 THEN 'Bought' ELSE 'Produced' END,
            CASE WHEN memory = 0 THEN 'Bought' ELSE 'Produced' END,
            CASE WHEN board = 0 THEN 'Bought' ELSE 'Produced' END
        FROM
            raw_comp_data;
        """
        self.con.sql(sql)
    
    def load_years_table(self, schema):
        sql = f"""
        WITH RECURSIVE years AS (
            SELECT 1970 as year
            UNION ALL
            SELECT year + 1
            FROM years
            WHERE year < 2000
        )
        SELECT 
            year,
            CASE 
                WHEN MOD(year - 1970, 12) = 0 THEN 'Dog'
                WHEN MOD(year - 1970, 12) = 1 THEN 'Pig'
                WHEN MOD(year - 1970, 12) = 2 THEN 'Rat'
                WHEN MOD(year - 1970, 12) = 3 THEN 'Ox'
                WHEN MOD(year - 1970, 12) = 4 THEN 'Tiger'
                WHEN MOD(year - 1970, 12) = 5 THEN 'Rabbit'
                WHEN MOD(year - 1970, 12) = 6 THEN 'Dragon'
                WHEN MOD(year - 1970, 12) = 7 THEN 'Snake'
                WHEN MOD(year - 1970, 12) = 8 THEN 'Horse'
                WHEN MOD(year - 1970, 12) = 9 THEN 'Sheep'
                WHEN MOD(year - 1970, 12) = 10 THEN 'Monkey'
                WHEN MOD(year - 1970, 12) = 11 THEN 'Rooster'
            END as zodiac_type
        FROM years;
        """
        self.con.sql(sql)   
    
    def load_workstation_sales_data(self, schema):
        sql = f"""
        INSERT INTO {schema}.workstation_sales 
            (year_id, company_id, integration_id, product_offering_id, sales, research_budget, employee_count, product_offering_count, product_category_count)
        SELECT
            year,
            MD5(COALESCE(
                COALESCE(firmname, '-1') ||
                COALESCE(CAST(CUSIPHEADERID AS VARCHAR), '-1') ||
                COALESCE(CAST(firmid AS VARCHAR), '-1') ||
                COALESCE(CAST(found AS VARCHAR), '-1') ||
                COALESCE(CAST(own AS VARCHAR), '-1') ||
                COALESCE(CAST(fate AS VARCHAR), '-1') ||
                COALESCE(CAST(estate AS VARCHAR), '-1') ||
                COALESCE(ceo, '-1') ||
                COALESCE(zip, '-1')
            , '-1')) as company_id,
            MD5(
                CASE WHEN cpu = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN os = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN apps = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN comm = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN disk = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN memory = 0 THEN 'Bought' ELSE 'Produced' END || 
                CASE WHEN board = 0 THEN 'Bought' ELSE 'Produced' END
            ) as integration_id,
            MD5(COALESCE(
                CASE
                    WHEN system = 1 AND graphic = 0 THEN 'Desktop systems'
                    WHEN system = 0 AND graphic = 1 THEN 'Graphics systems'
                    WHEN system = 1 AND graphic = 1 THEN 'Desktop and Graphics systems'
                    ELSE NULL
                END,
                '-1'
            )) as product_offering_id,
            sales,
            randd,
            employ,
            products,
            ptypes
        FROM
            raw_comp_data;
        """
        self.con.sql(sql)
