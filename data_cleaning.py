# import database_utils , data_extraction
# util_connector = database_utils.DatabaseConnector()
# source_table = util_connector.list_db_tables()
# engine = util_connector.init_db_engine()
# extraction_connector = data_extraction.DataExtractor(engine, source_table)

class DataCleaning:
    '''
    This class is used to clean data from each of the data sources.
    
    Attributes:

    '''
    def __init__(self):
        import database_utils
        util_connector = database_utils.DatabaseConnector()
        self.engine = util_connector.init_db_engine()
        self.source_table = util_connector.list_db_tables()
    
    def clean_user_data(): ##==== This function needs to be changed, don't need line 22-27, function take user data frame as argument ===
        import pandas as pd
        import database_utils , data_extraction
        util_connector = database_utils.DatabaseConnector()
        source_table = util_connector.list_db_tables()
        engine = util_connector.init_db_engine()
        extraction_connector = data_extraction.DataExtractor(engine, source_table)

        not_allow_char_string = "[!?*&1234567890]"
        allowed_country_list = ["United Kingdom", "Germany", "United States"]
        country_and_code_mapping = {"Germany" : "DE" , "United Kingdom" : "GB" , "United States" : "US"}
        code_and_country_mapping = {"DE" : "Germany" , "GB" : "United Kingdom" , "US" : "United States"}
        user_uui_length = 36
        user_data = extraction_connector.read_rds_table()
        
        #Convert data frame to string type
        user_data = user_data.astype("string")
        
        #Remove white space from columns
        user_data['first_name'] = user_data['first_name'].str.strip()
        user_data['last_name'] = user_data['last_name'].str.strip()
        user_data['email_address'] = user_data['email_address'].str.strip()
        user_data['user_uuid'] = user_data['user_uuid'].str.strip()
        
        #Correct country code under each valid country
        user_data['country_code'] = user_data["country"].replace(country_and_code_mapping)
        
        #correct country under each valid country code
        user_data['country'] = user_data["country_code"].replace(code_and_country_mapping)

        #drop the rows where country not in the allowed list, "~" is used for NOT isin()
        user_data = user_data.drop(user_data[~user_data.country.isin(allowed_country_list)].index)
                
        #Drop rows for first_name and last_name where contains unwanted characters
        user_data= user_data.drop(user_data[user_data.first_name.str.contains(not_allow_char_string, na=False)].index)
        user_data= user_data.drop(user_data[user_data.last_name.str.contains(not_allow_char_string, na=False)].index)
        
        #Change email_address value to None if email string does NOT contain "@"
        user_data.loc[user_data["email_address"].str.contains("@") == False, "email_address"] = None
        
        #For Germany phone numbers, remove +49, (, ) and spaces
        user_data.loc[user_data['country_code'] == 'DE', 'phone_number'] = user_data['phone_number'].replace({r'\+49': '', r'\ ': '', r'\(': '', r'\)': ''}, regex=True)

        #For US phone numbers, remove +1-, 001-, -, ., (, ) and spaces
        user_data.loc[user_data['country_code'] == 'US', 'phone_number'] = user_data['phone_number'].replace({r'\+1\-': '', r'\ ': '', r'001-': '', r'\-': '', r'\.': '', r'\(': '', r'\)': ''}, regex=True)
                        
        #For GB phone numbers, replace +44 with "", replace 442 with 02, replace +441 with 01, replace +442 with 02, replace +443 with 03, replace +448 with 08, replace +449 with 09, remove -, ., (, ) and spaces
        user_data.loc[user_data['country_code'] == 'GB', 'phone_number'] = user_data['phone_number'].replace({r'\+44': '', r'\ ': '', r'\-': '', r'\.': '', r'\+442': '02', r'442': '02', r'\+441': '01', r'\+443': '03', r'\+448': '08', r'\+449': '09', r'\(': '', r'\)': ''}, regex=True)
        
        #check if date is valid (empty? correct format?)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format = 'mixed', errors='coerce')
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], format = 'mixed', errors='coerce')
                
        #In address column, replace all next line "\n" with space " "?
        user_data.loc[ :, 'address'] = user_data['address'].replace({r'\n': ' '}, regex=True)
        
        #check length of user_uuid and drop all those in wrong format
        user_data= user_data.drop(user_data[user_data['user_uuid'].str.len() != user_uui_length].index)
        
        #check if email duplicate
        #check if address begin with 0
        return user_data
    
    def clean_card_data():##==== This function needs to be changed, don't need line 85-91, function take card data frame as argument ===
        import pandas as pd
        import database_utils , data_extraction
        pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        util_connector = database_utils.DatabaseConnector()
        source_table = util_connector.list_db_tables()
        engine = util_connector.init_db_engine()
        Connector = data_extraction.DataExtractor(engine, source_table)
        card_details = Connector.retrieve_pdf_data(pdf_link)
        #Convert data frame to string type
        card_details = card_details.astype("string")
        #Convert date_payment_confirmed to datetime64
        card_details['date_payment_confirmed'] = pd.to_datetime(card_details['date_payment_confirmed'], format = 'mixed', errors='coerce')
        #For card_number column, replace "?" with "" (remove all ?)
        card_details.loc[ :, 'card_number'] = card_details['card_number'].replace({r'\?': ''}, regex=True)
        #For card_number column, drop all those contains non-numeric cells
        card_details = card_details.drop(card_details[card_details["card_number"].str.isnumeric() == False].index)       
        #drop all NULL values
        card_details = card_details.dropna()
        return card_details
    
    def clean_store_data():##==== This function needs to be changed, don't need line 106-115, function take store data frame as argument ===
        import pandas as pd
        import data_extraction, database_utils
        util_connector = database_utils.DatabaseConnector()
        source_table = util_connector.list_db_tables()
        engine = util_connector.init_db_engine()
        extract_connector = data_extraction.DataExtractor(engine, source_table)
        store_number_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
        headers = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
        number_stores = extract_connector.list_number_of_stores(store_number_endpoint, headers)
        retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'+str(number_stores)
        store_data = extract_connector.retrieve_stores_data(retrieve_store_endpoint, headers)
        store_details = pd.DataFrame(store_data)
        store_code_length = 11
        store_type_list = ["Local", "Super Store", "Mall Kiosk", "Outlet"]
        country_and_continent_mapping = {"GB" : "Europe" , "DE" : "Europe" , "US" : "America"}
        country_list = []
        continent_list = []
        for key, value in country_and_continent_mapping.items():
            country_list.append(key)
            continent_list.append(value)
        continent_list = list(set(continent_list))
        
        #Convert data frame to string type
        store_details = store_details.astype("string")
        #Convert opening_date to datetime64
        store_details['opening_date'] = pd.to_datetime(store_details['opening_date'], format = 'mixed', errors='coerce')
        #For longitude column, drop all those contains non-numeric cells and convert to float64
        store_details = store_details.drop(store_details[store_details["longitude"].str.contains('[^0-9^\.]')].index)
        store_details["longitude"] = store_details["longitude"].astype("float64")
        #For latitude column, drop all those contains non-numeric cells and convert to float64
        store_details = store_details.drop(store_details[store_details["latitude"].str.contains('[^0-9^\.\-]')].index)
        store_details["latitude"] = store_details["latitude"].astype("float64")
        
        #For locality column, drop all those contains numeric cells
        store_details = store_details.drop(store_details[store_details["locality"].str.contains('\d') == True].index)
                
        #For store_code column, check if code format first 3 characters = first 2 characters of locality + "-", drop all not meeting requirement
        store_details = store_details.drop(store_details[store_details['store_code'].str[:3] != store_details['locality'].str[:2].str.upper() + '-'].index)
        #For store_code column, check if code length = 11, drop all not meeting requirement
        store_details = store_details.drop(store_details[store_details['store_code'].str.len() != store_code_length].index)
        #For staff_numbers, remove all none-digits
        store_details.loc[ :, 'staff_numbers'] = store_details['staff_numbers'].replace({r'\D': ''}, regex=True)
        #For store_type column, drop those not in the store_type_list
        store_details = store_details.drop(store_details[~store_details.store_type.isin(store_type_list)].index)
        #For country_code column, drop those not in the country_list
        store_details = store_details.drop(store_details[~store_details.country_code.isin(country_list)].index)
        #Correct continent for each valid country
        store_details['continent'] = store_details["country_code"].replace(country_and_continent_mapping)
        #For continent column, drop those not eligible
        store_details = store_details.drop(store_details[~store_details.continent.isin(continent_list)].index)
        #remove lat column and index column
        store_details = store_details.drop('lat', axis=1)
        store_details = store_details.drop("index", axis=1) 
        #drop all NULL values
        store_details = store_details.dropna()       

        return store_details
    
    def convert_product_weights(product_df):
        import pandas as pd
        product_df[["value", "weight_unit"]] = product_df["weight"].str.extract('(\d+\.\d+|\d+|\d+\s[x]\s\d+)(kg|g|ml|oz)', expand=True)
        product_df[["item_numbers", "item_weight"]] = product_df["value"].str.split(' x ', expand=True)
        product_df["item_numbers"] = product_df["item_numbers"].astype('float')
        product_df["item_weight"] = product_df["item_weight"].astype('float')
        product_df["total_weight"] = (product_df["item_numbers"] * product_df["item_weight"])/1000 #total weight in kg = number of items x unit weight / 1000
        product_df.loc[product_df["weight_unit"] == "oz", "total_weight"] = product_df["item_numbers"] / 35.274 #convert oz to kg
        product_df.loc[product_df["weight_unit"] == "ml", "total_weight"] = product_df["item_numbers"] / 1000 #convert ml to kg in 1:1 ratio of ml to g
        product_df.loc[product_df["total_weight"].isnull(), "total_weight"] = product_df["item_numbers"]
        product_df["weight_unit"] = "kg"
        unit_column = product_df.pop("weight_unit")
        product_df.insert(4, "weight_unit", unit_column)
        product_df["weight"] = product_df["total_weight"]
        product_df = product_df.drop(["value", "item_numbers", "item_weight", "total_weight"], axis=1)
        return product_df
    
    def clean_products_data(product_df):
        import pandas as pd
        uuid_length = 36
        #Start from removed column, remove rows not in the available list
        removed_options = ["Still_avaliable", "Removed"]
        product_df = product_df.drop(product_df[~product_df.removed.isin(removed_options)].index)
        #Processing price with currency symbol
        product_df[["currency", "price"]] = product_df["product_price"].str.extract('(£|$|€)(\d+\.\d\d)', expand=True)
        currency_column = product_df.pop("currency")
        product_df.insert(3, "currency", currency_column)  
        product_df["product_price"] = product_df["price"]
        product_df = product_df.drop(["price"], axis=1)
        product_df["product_price"] = product_df["product_price"].astype("float", decimal=2)
        #check length of product uuid and drop all those in wrong format
        product_df= product_df.drop(product_df[product_df['uuid'].str.len() != uuid_length].index)
        #Convert date_added to datetime64
        product_df['date_added'] = pd.to_datetime(product_df['date_added'], format = 'mixed', errors='coerce')
        #EAN code, remove all non numeric characters
        product_df = product_df.drop(product_df[product_df["EAN"].str.contains('[^0-9]')].index)
        #drop all NULL values
        product_df = product_df.dropna()
        #drop the first column
        product_df.drop(product_df.iloc[:,:1], inplace=True, axis=1)     
        return product_df

    def clean_orders_data(self, order_df):
        import pandas as pd
        order_df = order_df.drop(columns=["first_name", "last_name", "1"])
        return order_df

    def clean_events_time_data(self, date_data_df):
        import pandas as pd
        allowed_options = ["Evening", "Morning", "Midday", "Late_Hours"]
        date_data_df = date_data_df.drop(date_data_df[~date_data_df.time_period.isin(allowed_options)].index)        
        date_data_df["event_date"] = date_data_df["year"].astype(str)+"-"+ date_data_df["month"].astype(str)+"-"+ date_data_df["day"].astype(str) +" "+ date_data_df["timestamp"].astype(str)
        date_data_df["event_date"] = pd.to_datetime(date_data_df['event_date'], format='%Y-%m-%d %H:%M:%S')
        return date_data_df
        
#=============== for test only
#result = DataCleaning.clean_user_data()
#result.info()
# result.to_csv('data_clean_v1.csv', index=False)

#result = DataCleaning.clean_card_data()
# DataCleaning.clean_card_data()
# print(result)
# result.to_csv('data_clean_v1.csv', index=False)

#result = DataCleaning.clean_store_data()
#result.to_csv('store_clean_v1.csv', index=False)