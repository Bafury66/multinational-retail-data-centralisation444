# import database_utils
# import pandas as pd
# util_connector = database_utils.DatabaseConnector()
# source_table = util_connector.list_db_tables()
# engine = util_connector.init_db_engine()

class DataExtractor:
    '''
    This class work as a utility class enables methods that help extract data from different data sources.
    
    Attributes:

    '''
    def __init__(self):
         import database_utils
         util_connector = database_utils.DatabaseConnector()
         self.engine = util_connector.init_db_engine()
         self.source_table = util_connector.list_db_tables()
         #self.pdf_link = pdf_link
        
    def read_rds_table(self, type):
        import pandas as pd
        for table in self.source_table:
            if type in table:
                table_name = table
                data = pd.read_sql_table(table_name, self.engine).set_index('index')
                return data
        
    def retrieve_pdf_data(self, pdf_link): # to allow this method, tabula-py Python package has to be installed
        import tabula
        pdf_df = tabula.read_pdf(
            pdf_link,
            pages="all",
            lattice=True,
            multiple_tables=False,
        )                
        return pdf_df[0]
    
    def list_number_of_stores(self, store_number_endpoint, headers): #retrive number of stores to extract
        import requests
        response = requests.get(store_number_endpoint, headers= headers)
        if response.status_code == 200:
            data = response.json()
            number_of_store = data["number_stores"]
            return number_of_store
        else:
            print('Not getting correct response from server..')
            print(f"Response Text: {response.text}")

    def retrieve_stores_data(self, retrieve_store_endpoint, headers):
        import requests, json
        
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
        
        for store in range(0, store_number_int, 1):
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
        return store_data_list
    
    def extract_from_s3(self, s3_address):
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
    
    def extract_from_s3_url(self, s3url_address):
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
        
    '''
    def read_csv(self,):
        """
        This function extract data from CSV source.
        Args: 
            guess: the letter input from player
        """

    def read_API(self,):
        """
        This function extract data via API source.
        Args: 
            guess: the letter input from player
        """
        
    def read_s3(self,):
        """
        This function extract data from AWS S3 source.
        Args: 
            guess: the letter input from player
        """
        '''


#print(f'Database has these tables: {table_list}')
#table_name = input("Please enter table name")
# user_data = DataExtractor.read_rds_table(table_list)
# user_data.head(10)

# pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# source_table = util_connector.list_db_tables()
# engine = util_connector.init_db_engine()
# Connector = DataExtractor(engine, source_table)
# result = Connector.retrieve_pdf_data(pdf_link)
# result.to_csv('data_clean_card_original.csv', index=False)

#== Get store number
# pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# source_table = util_connector.list_db_tables()
# engine = util_connector.init_db_engine()
# api_connector = DataExtractor(engine, source_table)
# store_number_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
# headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# api_connector.list_number_of_stores(store_endpoint, headers)

#== Retrieve stores data
# pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# source_table = util_connector.list_db_tables()
# engine = util_connector.init_db_engine()
# api_connector = DataExtractor(engine, source_table)
# store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/451'
# headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# api_connector.retrieve_stores_data(store_endpoint, headers)