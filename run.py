import data_cleaning, data_extraction, database_utils, pandas as pd
clean_connector = data_cleaning.DataCleaning()
extraction_connector = data_extraction.DataExtractor()
utils_connector = database_utils.DatabaseConnector()

def manual_grab_table():
    manual_list = ["card details from PDF", "store details from API", "product details from CSV", "date details from JSON" ]
    table_list = utils_connector.list_db_tables()
    for index, table in enumerate(table_list):
        table_list[index] = table + " from database"
    table_list = table_list + manual_list
    for index, table in enumerate(table_list):  
        print(f"{index+1} - {table}.")
        
    yet_to_select = True
    while yet_to_select == True:
        table_to_select = input(f'\nPlease select the table from list above to download, item 1 - {len(table_list)}, or x to cancel: ')
        if table_to_select == "x":
            table_to_download = "cancel"
            break
        elif len(table_to_select) != 1 or table_to_select.isalpha() == True:
            print(f"\nPlease select the index number from list above.")

        else:
            table_to_download = table_list[int(table_to_select)-1]
            print(f"\nYou have selected table named '{table_to_download}'.")
            print(f"\n'{table_to_download}' table is being downloaded, cleaned, and uploaded to your local database, please wait....")
            yet_to_select = False

    if table_to_download == "legacy_users from database":
        grab_user_details_from_db()
    elif table_to_download == "legacy_store_details from database":
        grab_store_details_from_db()
    elif table_to_download == "orders_table from database":
        grab_order_details_from_db()
    elif table_to_download == "card details from PDF":
        grab_card_details_from_pdf_link()
    elif table_to_download == "store details from API":
        grab_store_details_with_api()
    elif table_to_download == "product details from CSV":
        grab_product_details_from_csv()         
    elif table_to_download == "date details from JSON":
        grab_date_details_from_json()  
    else:
        print(f"\nYou have cancelled the operation.")

def grab_user_details_from_db():
    user_table = extraction_connector.read_rds_table(utils_connector, "legacy_users")
    cleaned_user_table = clean_connector.clean_user_data(user_table)
    utils_connector.upload_to_db(cleaned_user_table, "dim_users")    

def grab_order_details_from_db():
    order_table = extraction_connector.read_rds_table(utils_connector, "orders_table")
    cleaned_order_table = clean_connector.clean_orders_data(order_table)
    utils_connector.upload_to_db(cleaned_order_table, "orders_table")
    
def grab_store_details_from_db():
    store_table = extraction_connector.read_rds_table(utils_connector, "legacy_store_details")
    cleaned_store_table = clean_connector.clean_store_data(store_table)
    utils_connector.upload_to_db(cleaned_store_table, "dim_store_details")    

def grab_card_details_from_pdf_link():   
    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    card_details = extraction_connector.retrieve_pdf_data(pdf_link)
    cleaned_card_details = clean_connector.clean_card_data(card_details)
    utils_connector.upload_to_db(cleaned_card_details, "dim_card_details")

def grab_store_details_with_api(): 
    headers = utils_connector.read_store_api_creds()[0]
    store_number_endpoint = utils_connector.read_store_api_creds()[1]
    retrieve_store_endpoint = utils_connector.read_store_api_creds()[2]
    number_stores = extraction_connector.list_number_of_stores(store_number_endpoint, headers)
    print(f'\nThere are total of {number_stores} set of records, all of their details will be extracted.')
    print(f'\nPlease wait a moment: \n')
    retrieve_store_endpoint = retrieve_store_endpoint.replace("{store_number}", str(number_stores))

    store_details = pd.DataFrame(extraction_connector.retrieve_stores_data(retrieve_store_endpoint, headers))
    cleaned_store_details = clean_connector.clean_store_data(store_details)
    utils_connector.upload_to_db(cleaned_store_details, "dim_store_details")

def grab_product_details_from_csv():     
    s3_address = "s3://data-handling-public/products.csv"
    product_raw_data = extraction_connector.extract_from_s3(s3_address)
    product_weight_cleaned = clean_connector.convert_product_weights(product_raw_data)
    cleaned_product_details = clean_connector.clean_products_data(product_weight_cleaned)
    utils_connector.upload_to_db(cleaned_product_details, "dim_products")
    
def grab_date_details_from_json():
    s3url_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    date_raw_data = extraction_connector.extract_from_s3_url(s3url_address)
    cleaned_date_details = clean_connector.clean_events_time_data(date_raw_data)
    utils_connector.upload_to_db(cleaned_date_details, "dim_date_times")

def run_the_project():
    auto_or_manual_run = input(f'Please select "1" to download and update all 6 tables automatically, "2" for manual selection.')
    condition = True
    while condition == True:
        if auto_or_manual_run not in ["1", "2", "X", "x"]:
            print(f'\nOption selected is invalid, please select "1" or "2" to proceed, or "X" to cancel.')
        elif auto_or_manual_run == "1":
            grab_user_details_from_db()
            grab_card_details_from_pdf_link()
            grab_store_details_with_api()
            grab_product_details_from_csv()
            grab_order_details_from_db()
            grab_date_details_from_json()
            break
        elif auto_or_manual_run == "2":
            manual_grab_table()
            break
        else:
            print(f"\nYou have cancelled the operation.")
            break

run_the_project()