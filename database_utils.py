class DatabaseConnector:
    '''
    This class is used to connect with and upload data to the database.
    
    Attributes:

    '''
    def __init__(self):
         self.db_creds = self.read_db_creds()
        
    def read_db_creds(self):
        """
        This function open and read the credentials yaml file and return a dictionary of the credentials.
        For this to work, PyYAML have to be installed via pip install PyYAML
        Args: 
            
        """
        import yaml
        with open('db_creds.yaml', mode='r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    def init_db_engine(self):
        """
        This function read the credentials from the return of read_db_creds 
        and initialise and return an sqlalchemy database engine.
        This require (pip install sqlalchemy bforehand)
        Args: 
        """
        db_creds = self.read_db_creds()
        from sqlalchemy import create_engine
        database_type = 'postgresql'
        dbapi = 'psycopg2'
        host = db_creds['RDS_HOST']
        user = db_creds['RDS_USER']
        password = db_creds['RDS_PASSWORD']
        database = db_creds['RDS_DATABASE']
        port = db_creds['RDS_PORT']
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
        return engine

    def list_db_tables(self):
        """
        This function use the database engine to list all the tables in the database.
        Args: 
        """
        engine = self.init_db_engine()
        from sqlalchemy import inspect
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = inspect(engine)
        return inspector.get_table_names()

    def pdf_link(self):
        """
        This function ask for an link for PDF source.
        Args: 
        """
        pdf_link = input("Please enter PDF link..: ")
        return pdf_link
    
    def upload_to_db(self, data_frame, table_name):
        from sqlalchemy import create_engine
        from sklearn.datasets import load_iris
        local_db_creds = self.read_local_db_creds()
        database_type = local_db_creds['LDS_DATABASE_TYPE']
        dbapi = local_db_creds['LDS_DBAPI']
        host = local_db_creds['LDS_HOST']
        user = local_db_creds['LDS_USER']
        password = local_db_creds['LDS_PASSWORD']
        database = local_db_creds['LDS_DATABASE']
        port = local_db_creds['LDS_PORT']
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        data_frame.to_sql(table_name, engine, if_exists='replace')
        #engine.close()

    def read_local_db_creds(self):
        """
        This function open and read the credentials yaml file and return a dictionary of the credentials for local DB.
        For this to work, PyYAML have to be installed via pip install PyYAML
        Args: 
            
        """
        import yaml
        with open('local_db_creds.yaml', mode='r') as file:
            local_db_creds = yaml.safe_load(file)
        return local_db_creds
    
    def read_store_api_creds(self):
        """
        This function open and read the credentials yaml file and return a dictionary of the end point and API key for store table.
        For this to work, PyYAML have to be installed via pip install PyYAML
        Args: 
            
        """
        import yaml
        with open('store_api_creds.yaml', mode='r') as file:
            local_db_creds = yaml.safe_load(file)
        return local_db_creds
        
#==================Below are used for test functions==== Remove after code completed
# table_list = DatabaseConnector().list_db_tables()
# print(f'Database has these tables: {table_list}')

# engine.execution_options(isolation_level='AUTOCOMMIT').connect()
# inspector = inspect(engine)
# return inspector.get_table_names()

#===== upload pdf with card_details to db
# import data_cleaning
# data_frame = data_cleaning.DataCleaning.clean_card_data()
# table_name = "dim_card_details"
# DatabaseConnector().upload_to_db(data_frame, table_name)

# ===== upload csv to db
# import data_cleaning
# data_frame = data_cleaning.DataCleaning.clean_user_data()
# table_name = "dim_users"
# DatabaseConnector().upload_to_db(data_frame, table_name)

# ===== upload cleaned store detail to db
# import data_cleaning
# data_frame = data_cleaning.DataCleaning.clean_store_data()
# table_name = "dim_store_details"
# DatabaseConnector().upload_to_db(data_frame, table_name)

