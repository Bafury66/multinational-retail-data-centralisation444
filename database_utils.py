class DatabaseConnector:
    '''
    This class is used to connect with and upload data to the database.
    
    Attributes: All methods used here are static method.

    '''
    @staticmethod  
    def read_db_creds():
        '''
        This function opens and reads the credentials yaml file in the naming format of "db_creds.yaml" and return a dictionary of the credentials.
        For this to work, PyYAML have to be installed via pip install PyYAML on the terminal.
        Returns:
            db_creds: a YAML object contains the credentials for AWS RDS database
        '''   
        import yaml
        with open('db_creds.yaml', mode='r') as file:
            db_creds = yaml.safe_load(file)
        return db_creds
    
    @staticmethod  
    def init_db_engine():
        '''
        This function read the credentials from the return of read_db_creds, and initialise then return an sqlalchemy database engine.
        For this to work, it requires (pip install sqlalchemy bforehand).
        Args: 
            None, assuming this is only used to create specific AWS RDS database engine.
        Returns:
            engine: A sqlalchemy database engine used to open a connection between local machine and AWS RDS database.
        '''   
        from sqlalchemy import create_engine
        db_creds = DatabaseConnector.read_db_creds()
        database_type = 'postgresql'
        dbapi = 'psycopg2'
        host = db_creds['RDS_HOST']
        user = db_creds['RDS_USER']
        password = db_creds['RDS_PASSWORD']
        database = db_creds['RDS_DATABASE']
        port = db_creds['RDS_PORT']
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}")
        return engine

    @staticmethod  
    def list_db_tables():
        '''
        This function use thes database engine to connect to database then list all the tables available on the database.
        Args: 
            None, assuming this is only used to create specific AWS RDS database engine.
        Returns:
            table_list: A list of all the tables available on the AWS RDS database.
        '''   
        from sqlalchemy import inspect
        engine = DatabaseConnector.init_db_engine()
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = inspect(engine)
        table_list = inspector.get_table_names() 
        return table_list

    @staticmethod
    def pdf_link():
        '''
        This function ask for a link from user for PDF source.
        Returns: pdf_link ready to be used to grab data. 
        '''
        pdf_link = input("Please enter PDF link..: ")
        return pdf_link
    
    @staticmethod
    def upload_to_db(data_frame, table_name):
        '''
        This function is used to upload the dataframe to local database.
        Args:
            data_frame - the data frame ready to upload.
            table_name - the name of table shown on database.
        Returns: None 
        '''
        from sklearn.datasets import load_iris
        from sqlalchemy import create_engine
        local_db_creds = DatabaseConnector.read_local_db_creds()
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
        print(f'\n"{table_name}" Table Uploaded/Replaced Successfully..\n')
        
    @staticmethod
    def read_local_db_creds():
        '''
        This function opens and reads the credentials yaml file in the naming format of "local_db_creds.yaml" and return a dictionary of the credentials.
        Credentials are login details for local database.
        For this to work, PyYAML have to be installed via pip install PyYAML on the terminal.
        Args: 
            None
        Returns:
            local_db_creds: a YAML object contains the credentials for AWS RDS database
        '''   
        import yaml
        with open('local_db_creds.yaml', mode='r') as file:
            local_db_creds = yaml.safe_load(file)
        return local_db_creds
    
    @staticmethod
    def read_store_api_creds():
        '''
        This function open and read the credentials yaml file in the naming format of "store_api_creds.yaml" and
        return a dictionary of the end point and API key for accessing store table.
        Credentials are API headers with key and value pair, store number end point and retrieve store data paths.
        For this to work, PyYAML have to be installed via pip install PyYAML on the terminal.
        Args: 
            None
        Returns:
            api_creds: a YAML object contains the credentials for AWS database
        '''  
        import yaml
        with open('store_api_creds.yaml', mode='r') as file:
            api_creds = yaml.safe_load(file)
        headers = api_creds["headers"][0]
        store_number_endpoint = api_creds["store_number_endpoint"]
        retrieve_store_endpoint = api_creds["retrieve_store_endpoint"]
        return headers, store_number_endpoint, retrieve_store_endpoint
