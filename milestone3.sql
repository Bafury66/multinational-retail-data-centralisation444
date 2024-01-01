-- Task 1:
ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid
ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid
ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(19)
ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(12)
ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(11)
ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT

-- Task 2:
ALTER TABLE dim_users ALTER COLUMN first_name TYPE VARCHAR(255)
ALTER TABLE dim_users ALTER COLUMN last_name TYPE VARCHAR(255)
ALTER TABLE dim_users ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE
ALTER TABLE dim_users ALTER COLUMN country_code TYPE VARCHAR(2)
ALTER TABLE dim_users ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid
ALTER TABLE dim_users ALTER COLUMN join_date TYPE DATE USING join_date::DATE

-- Task 3:
UPDATE dim_store_details
    SET lat = concat_ws('/', latitude, lat);

ALTER TABLE dim_store_details
    DROP column lat;

UPDATE dim_store_details
    SET longitude = NULL
	WHERE longitude = 'N/A';

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE float USING longitude::float;

ALTER TABLE dim_store_details 
    ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details 
    ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details 
    ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT;

ALTER TABLE dim_store_details 
    ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE;

ALTER TABLE dim_store_details 
    ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details 
    ALTER COLUMN store_type SET NOT NULL;

ALTER TABLE dim_store_details 
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;

ALTER TABLE dim_store_details 
    ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details 
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Task 4:
UPDATE dim_products
    SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
	ADD COLUMN weight_class VARCHAR(14);

UPDATE dim_products
    SET	weight_class = 'Light'
    WHERE weight < 2;

UPDATE dim_products
    SET	weight_class = 'Mid_Sized'
    WHERE weight >= 2 and weight < 40;

UPDATE dim_products
    SET	weight_class = 'Heavy'
    WHERE weight >= 40 and weight < 140;

UPDATE dim_products
    SET	weight_class = 'Truck_Required'
    WHERE weight >= 140;

-- Task 5:
ALTER TABLE dim_products
  RENAME COLUMN removed TO still_available;

UPDATE dim_products
    SET
        still_available = 'Yes'
    WHERE
        still_available = 'Still_avaliable';

UPDATE dim_products
SET
	still_available = 'No'
WHERE
	still_available = 'Removed'

ALTER TABLE dim_products 
    ALTER COLUMN still_available TYPE BOOL USING still_available::bool;

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;

ALTER TABLE dim_products
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

ALTER TABLE dim_products 
    ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products 
    ALTER COLUMN "product_code" TYPE VARCHAR(11);

ALTER TABLE dim_products 
    ALTER COLUMN date_added TYPE DATE USING date_added::DATE;

ALTER TABLE dim_products 
    ALTER COLUMN uuid TYPE uuid USING uuid::uuid;

ALTER TABLE dim_products 
    ALTER COLUMN "weight_class" TYPE VARCHAR(14);

-- Task 6:
ALTER TABLE dim_date_times 
    ALTER COLUMN "month" TYPE VARCHAR(2);

ALTER TABLE dim_date_times 
    ALTER COLUMN "year" TYPE VARCHAR(4);

ALTER TABLE dim_date_times 
    ALTER COLUMN "day" TYPE VARCHAR(2);

ALTER TABLE dim_date_times 
    ALTER COLUMN "time_period" TYPE VARCHAR(10);

ALTER TABLE dim_date_times 
    ALTER COLUMN "date_uuid" TYPE uuid USING date_uuid::uuid;

-- Task 7:
ALTER TABLE dim_card_details 
    ALTER COLUMN "card_number" TYPE VARCHAR(19);

ALTER TABLE dim_card_details 
    ALTER COLUMN "expiry_date" TYPE VARCHAR(5);

ALTER TABLE dim_card_details 
    ALTER COLUMN "date_payment_confirmed" TYPE DATE USING date_payment_confirmed::DATE;

-- Task 8:

ALTER TABLE dim_users
    ADD primary key(user_uuid);

ALTER TABLE dim_store_details
    ADD primary key(store_code);

ALTER TABLE dim_products
    ADD primary key(product_code);

ALTER TABLE dim_date_times 
    ADD primary key (date_uuid);

ALTER TABLE dim_card_details
    ADD primary key (card_number);

-- Task 9:
ALTER TABLE orders_table
    ADD CONSTRAINT fk_date_uuid
    FOREIGN KEY (date_uuid)
    REFERENCES dim_date_times (date_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_user_uuid
    FOREIGN KEY (user_uuid)
    REFERENCES dim_users (user_uuid);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_card_number
    FOREIGN KEY (card_number)
    REFERENCES dim_card_details (card_number);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_store_code
    FOREIGN KEY (store_code)
    REFERENCES dim_store_details (store_code);

ALTER TABLE orders_table
    ADD CONSTRAINT fk_product_key
    FOREIGN KEY (product_code)
    REFERENCES dim_products (product_code);
