class DataCleaning:
    '''
    This class is used to clean dataframe gathered from each of the data sources.
    
    Attributes: All methods used here are static method.

    '''
    @staticmethod    
    def clean_user_data(user_data):
        '''
        This function is used to clean the user data frame. This function assumes the data format downloaded is always the same,
        i.e. column names do not change from time to time.

        Args:
            user_data: The user data from data source in the form of pandas dataframe.

        Returns:
            user_data: A data cleaned dataframe ready to upload to local database.
        '''        
        import pandas as pd
        not_allow_char_string = "[!?*&1234567890]"
        allowed_country_list = ["United Kingdom", "Germany", "United States"]
        country_and_code_mapping = {"Germany" : "DE" , "United Kingdom" : "GB" , "United States" : "US"}
        code_and_country_mapping = {"DE" : "Germany" , "GB" : "United Kingdom" , "US" : "United States"}
        user_uui_length = 36
        user_data = user_data.astype("string")
        user_data['first_name'] = user_data['first_name'].str.strip()
        user_data['last_name'] = user_data['last_name'].str.strip()
        user_data['email_address'] = user_data['email_address'].str.strip()
        user_data['user_uuid'] = user_data['user_uuid'].str.strip()
        user_data['country_code'] = user_data["country"].replace(country_and_code_mapping)
        user_data['country'] = user_data["country_code"].replace(code_and_country_mapping)
        user_data = user_data.drop(user_data[~user_data.country.isin(allowed_country_list)].index)
        user_data= user_data.drop(user_data[user_data.first_name.str.contains(not_allow_char_string, na=False)].index)
        user_data= user_data.drop(user_data[user_data.last_name.str.contains(not_allow_char_string, na=False)].index)
        user_data.loc[user_data["email_address"].str.contains("@") == False, "email_address"] = None
        user_data.loc[user_data['country_code'] == 'DE', 'phone_number'] = user_data['phone_number'].replace({r'\+49': '', r'\ ': '', r'\(': '', r'\)': ''}, regex=True)
        user_data.loc[user_data['country_code'] == 'US', 'phone_number'] = user_data['phone_number'].replace({r'\+1\-': '', r'\ ': '', r'001-': '', r'\-': '', r'\.': '', r'\(': '', r'\)': ''}, regex=True)
        user_data.loc[user_data['country_code'] == 'GB', 'phone_number'] = user_data['phone_number'].replace({r'\+44': '', r'\ ': '', r'\-': '', r'\.': '', r'\+442': '02', r'442': '02', r'\+441': '01', r'\+443': '03', r'\+448': '08', r'\+449': '09', r'\(': '', r'\)': ''}, regex=True)
        user_data['date_of_birth'] = pd.to_datetime(user_data['date_of_birth'], format = 'mixed', errors='coerce')
        user_data['join_date'] = pd.to_datetime(user_data['join_date'], format = 'mixed', errors='coerce')
        user_data.loc[ :, 'address'] = user_data['address'].replace({r'\n': ' '}, regex=True)
        user_data= user_data.drop(user_data[user_data['user_uuid'].str.len() != user_uui_length].index)
        return user_data

    @staticmethod
    def clean_card_data(card_details):
        '''
        This function is used to clean the card data frame. This function assumes the data format downloaded is always the same,
        i.e. column names do not change from time to time.

        Args:
            card_details: The card data from data source in the form of pandas dataframe.

        Returns:
            card_details: A data cleaned dataframe ready to upload to local database.
        '''   
        import pandas as pd
        card_details = card_details.astype("string")
        card_details['date_payment_confirmed'] = pd.to_datetime(card_details['date_payment_confirmed'], format = 'mixed', errors='coerce')
        card_details["card_number"] = card_details["card_number"].str.extract('(\d+)')
        card_details = card_details.drop(card_details[card_details["card_number"].str.isnumeric() == False].index)       
        card_details = card_details.dropna()
        return card_details
    
    @staticmethod
    def clean_store_data(store_df):
        '''
        This function is used to clean the store details data frame. This function assumes the data format downloaded is always the same,
        i.e. column names do not change from time to time.

        Args:
            store_df: The store data from data source in the form of pandas dataframe.

        Returns:
            store_details: A data cleaned dataframe ready to upload to local database.
        '''   
        import pandas as pd
        store_details = store_df
        store_code_length = 11
        store_type_list = ["Local", "Super Store", "Mall Kiosk", "Outlet", "Web Portal"]
        country_and_continent_mapping = {"GB" : "Europe" , "DE" : "Europe" , "US" : "America"}
        country_list = []
        continent_list = []
        for key, value in country_and_continent_mapping.items():
            country_list.append(key)
            continent_list.append(value)
        continent_list = list(set(continent_list))
        store_details = store_details.astype("string")
        store_details['opening_date'] = pd.to_datetime(store_details['opening_date'], format = 'mixed', errors='coerce')
        store_details = store_details.drop(store_details[store_details["locality"].str.contains('\d') == True].index)
        store_details.loc[ :, 'staff_numbers'] = store_details['staff_numbers'].replace({r'\D': ''}, regex=True)
        store_details = store_details.drop(store_details[~store_details.store_type.isin(store_type_list)].index)
        store_details = store_details.drop(store_details[~store_details.country_code.isin(country_list)].index)
        store_details['continent'] = store_details["country_code"].replace(country_and_continent_mapping)
        store_details = store_details.drop(store_details[~store_details.continent.isin(continent_list)].index)
        try:
            store_details = store_details.drop("index", axis=1)
        except KeyError:
            pass
        return store_details
    
    @staticmethod  
    def convert_product_weights(product_df):
        '''
        This function is only used to convert the product weights from "g"(gram) OR "ml"(milliliter) to "kg"(Kilogram) via relevant calculations.
        
        Args:
            product_df: The product data from data source in the form of pandas dataframe.

        Returns:
            product_df: A product dataframe with unify weight unit (kg) ready to be further processed or cleaned.
        '''   
        import pandas as pd
        product_df[["value", "weight_unit"]] = product_df["weight"].str.extract('(\d+\.\d+|\d+|\d+\s[x]\s\d+)(kg|g|ml|oz)', expand=True)
        product_df[["item_numbers", "item_weight"]] = product_df["value"].str.split(' x ', expand=True)
        product_df["item_numbers"] = product_df["item_numbers"].astype('float')
        product_df["item_weight"] = product_df["item_weight"].astype('float')
        product_df["total_weight"] = (product_df["item_numbers"] * product_df["item_weight"])/1000
        product_df.loc[product_df["weight_unit"] == "oz", "total_weight"] = product_df["item_numbers"] / 35.274 
        product_df.loc[product_df["weight_unit"] == "ml", "total_weight"] = product_df["item_numbers"] / 1000 
        product_df.loc[product_df["total_weight"].isnull(), "total_weight"] = product_df["item_numbers"]
        product_df["weight_unit"] = "kg"
        unit_column = product_df.pop("weight_unit")
        product_df.insert(4, "weight_unit", unit_column)
        product_df["weight"] = product_df["total_weight"]
        product_df = product_df.drop(["value", "item_numbers", "item_weight", "total_weight"], axis=1)
        return product_df
    
    @staticmethod
    def clean_products_data(product_df):
        '''
        This function is used to clean the product details data frame. This function assumes the data format downloaded is always the same,
        i.e. column names do not change from time to time.

        Args:
            product_df: The product data from data source in the form of pandas dataframe.

        Returns:
            product_df: A data cleaned dataframe ready to upload to local database.
        '''   
        import pandas as pd
        uuid_length = 36
        removed_options = ["Still_avaliable", "Removed"]
        product_df = product_df.drop(product_df[~product_df.removed.isin(removed_options)].index)
        product_df[["currency", "price"]] = product_df["product_price"].str.extract('(£|$|€)(\d+\.\d\d)', expand=True)
        product_df= product_df.drop(product_df[product_df['uuid'].str.len() != uuid_length].index)
        product_df['date_added'] = pd.to_datetime(product_df['date_added'], format = 'mixed', errors='coerce')
        product_df = product_df.drop(product_df[product_df["EAN"].str.contains('[^0-9]')].index)
        product_df = product_df.dropna()
        product_df.drop(product_df.iloc[:,:1], inplace=True, axis=1)     
        return product_df

    @staticmethod
    def clean_orders_data(order_df):
        '''
        This function is used to clean the order details data frame. This function assumes the data format downloaded is always the same,
        i.e. column names do not change from time to time.

        Args:
            order_df: The order data from data source in the form of pandas dataframe.

        Returns:
            order_df: A data cleaned dataframe ready to upload to local database.
        '''  
        import pandas as pd
        order_df = order_df.drop(columns=["first_name", "last_name", "1"])
        return order_df
    
    @staticmethod
    def clean_events_time_data(date_data_df):
        '''
        This function is used to clean the date details data frame. This function assumes the data format downloaded is always the same,
        i.e. column names do not change from time to time.

        Args:
            date_data_df: The date data from data source in the form of pandas dataframe.

        Returns:
            date_data_df: A data cleaned dataframe ready to upload to local database.
        '''  
        import pandas as pd
        allowed_options = ["Evening", "Morning", "Midday", "Late_Hours"]
        date_data_df = date_data_df.drop(date_data_df[~date_data_df.time_period.isin(allowed_options)].index)        
        date_data_df["event_date"] = date_data_df["year"].astype(str)+"-"+ date_data_df["month"].astype(str)+"-"+ date_data_df["day"].astype(str) +" "+ date_data_df["timestamp"].astype(str)
        date_data_df["event_date"] = pd.to_datetime(date_data_df['event_date'], format='%Y-%m-%d %H:%M:%S')
        return date_data_df
    