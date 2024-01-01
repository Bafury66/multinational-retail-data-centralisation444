class DataExtractor:
    '''
    This class work as a utility class enables methods that help extract data from different data sources.
    
    Attributes:
    '''
    @staticmethod
    def read_rds_table(util_connector, table_name):
        '''
        This function extract the database table to a pandas DataFrame.
        Args: 
            util_connector:this is the DatabaseConnector class from database_utils module.
            table_name: table name to be extracted from the databse.
        Returns:
            db_creds: a YAML object contains the credentials for AWS RDS database
        '''  
        import pandas as pd
        engine = util_connector.init_db_engine()
        data = pd.read_sql_table(table_name, engine).set_index('index')
        return data
    
    @staticmethod        
    def retrieve_pdf_data(pdf_link):
        '''
        This function reads an url link of a pdf file and return a pandas DataFrame.
        To allow this method, tabula-py Python package has to be installed
        Args: 
            pdf_link: url link of a pdf file.
        Returns:
            pdf_df: a pandas DataFrame
        '''  
        import tabula
        pdf_raw_data = tabula.read_pdf(
            pdf_link,
            pages="all",
            lattice=True,
            multiple_tables=False,
        )
        pdf_df = pdf_raw_data[0]              
        return pdf_df
    
    @staticmethod
    def list_number_of_stores(store_number_endpoint, headers):
        '''
        This function retrieves and return the total number of stores available on the cloud through the use of API.
        Args: 
            store_number_endpoint: store number endpoint which holds the total number of stores.
            headers: headers with key and value pair info to be used during api connection
        Returns:
            number_of_store: Number of stores
        '''          
        import requests
        response = requests.get(store_number_endpoint, headers= headers)
        if response.status_code == 200:
            data = response.json()
            number_of_store = data["number_stores"]
            return number_of_store
        else:
            print('Not getting correct response from server..')
            print(f"Response Text: {response.text}")
            
    @staticmethod
    def retrieve_stores_data(retrieve_store_endpoint, headers):
        '''
        This function extracts all the stores from the API and save and return a pandas DataFrame.
        Args: 
            retrieve_store_endpoint: endpoint path where store details can be accessed.
            headers: headers with key and value pair info to be used during api connection
        Returns:
            store_df: a pandas DataFrame contains all the details of the stores availav.
        '''         
        from tqdm import tqdm
        import requests, pandas as pd
        string_length = len(retrieve_store_endpoint)
        for char in range(string_length-1, 0, -1):
            if retrieve_store_endpoint[char] == "/":
                last_slash_location = char
                break
        store_number_str = ''
        for num in range(last_slash_location + 1, string_length, 1):
            store_number_str = store_number_str + retrieve_store_endpoint[num]
        store_number_int = int(store_number_str)
        
        retrieve_store_endpoint_root = retrieve_store_endpoint[0:last_slash_location+1]
        
        store_data_list=[]
        for store in tqdm(range(0, store_number_int, 1), desc="Retrieving Data in Progress..", total = store_number_int):
            retrieve_store_endpoint_number = str(store)
            end_point_url = retrieve_store_endpoint_root + retrieve_store_endpoint_number
            
            response = requests.get(end_point_url, headers= headers)
            if response.status_code == 200:
                data = response.json()
                store_data_list.append(data)
            else:
                print(f"Response Text: {response.status_code}")
                print(f"Response Text: {response.text}")
                print('Not getting correct response from server..')
        store_df = pd.DataFrame(store_data_list)
        return store_df
    
    @staticmethod
    def extract_from_s3(s3_address):
        '''
        This function uses AWS SDK commands to read and extract data from the csv file held on Amazon S3 cloud storage 
        and returns a pandas DataFrame of product details. This requires AWS SDK for Python (Boto3) and will download a copy
        of CSV file to local drive where the python file held.
        Args: 
            s3_address: The S3 address for the products data which is s3://data-handling-public/products.csv .
        Returns:
            product_data: a pandas DataFrame contains all the details of the products.
        ''' 
        import boto3, pandas as pd
        new_string = s3_address.replace('s3://', '')
        bucket_end_position = new_string.rindex('/')
        bucket = new_string[:bucket_end_position]
        file_path = new_string[bucket_end_position+1:]
        s3 = boto3.client('s3')
        s3.download_file(bucket, file_path, './products.csv')
        product_data = pd.read_csv('products.csv')
        product_data.head()
        return product_data
    
    @staticmethod
    def extract_from_s3_url(s3url_address):
        '''
        This function uses AWS SDK commands to read and extract data from the JSON file held on Amazon S3 cloud storage 
        and returns a pandas DataFrame of date details. This requires AWS SDK for Python (Boto3) and will download a copy
        of JSON file to local drive where the python file held.
        Args: 
            s3url_address: The S3 address for the products data which is https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json .
        Returns:
            date_data: a pandas DataFrame contains all the details of the dates in record.
        '''         
        import boto3, pandas as pd
        new_string = s3url_address.replace('https://', '')
        bucket_end_position = new_string.rindex('.s3')
        file_name_start_position = new_string.rindex('/')
        bucket = new_string[:bucket_end_position]
        file_path = new_string[file_name_start_position+1:]
        s3 = boto3.client('s3')
        s3.download_file(bucket, file_path, './date_details.json')
        date_data = pd.read_json('date_details.json')
        return date_data
        