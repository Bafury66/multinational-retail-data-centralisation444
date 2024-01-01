-- Task 1: which countries we currently operate in and which country now has the most stores. 
SELECT
    country_code AS country, count(store_code) AS total_no_stores
FROM
    dim_store_details
WHERE
    store_type <> 'Web Portal'
GROUP BY
    country
ORDER BY
    total_no_stores DESC;

-- Task 2: which locations currently have the most stores.
SELECT
    locality, count(store_code) AS total_no_stores
FROM
    dim_store_details
WHERE
    store_type <> 'Web Portal'
GROUP BY
    locality
ORDER BY
    total_no_stores DESC
LIMIT
    7;

-- Task 3: Query the database to find out which months have produced the most sales.
SELECT
    ROUND(CAST(SUM(product_quantity * product_price) AS numeric),2) AS total_sales, dim_date_times.month as month
FROM
    orders_table
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    month
ORDER BY
    total_sales DESC
LIMIT
    6;

-- Task 4: Calculate how many products were sold and the amount of sales made for online and offline purchases.
SELECT
    COUNT(dim_store_details.store_code) AS numbers_of_sales, 
    SUM(product_quantity) AS product_quantity_count, 
    CASE store_type
    WHEN 'Web Portal' THEN 'Web'
    ELSE 'Offine'
    END AS location
FROM
    orders_table
JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY
    location
ORDER BY
    numbers_of_sales ASC;

-- Task 5: Find out the total and percentage of sales coming from each of the different store types.
SELECT 
    store_type, total_sales, ROUND((total_sales * 100 /SUM(total_sales) OVER()),2) AS "percentage_total(%)"
FROM
    (SELECT
        store_type, ROUND(CAST(SUM(product_quantity * product_price) AS numeric),2) AS total_sales
    FROM
        orders_table
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code
    JOIN
        dim_store_details ON orders_table.store_code = dim_store_details.store_code
    GROUP BY
        store_type) AS store_sales
GROUP BY
    store_sales.store_type, store_sales.total_sales
ORDER BY
    total_sales DESC;

-- Task 6: Find out the total and percentage of sales coming from each of the different store types.
SELECT
    ROUND(CAST(SUM(product_quantity * product_price) AS numeric),2) AS total_sales, dim_date_times.year as year, dim_date_times.month as month
FROM
    orders_table
JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    month, year
ORDER BY
    total_sales DESC
LIMIT
    10;

-- Task 7: Find out the the overall staff numbers in each location around the world.
SELECT
    SUM(staff_numbers) AS total_staff_numbers, country_code
FROM
    dim_store_details
GROUP BY
    country_code
ORDER BY
    total_staff_numbers DESC;

-- Task 8: Determine which type of store is generating the most sales in Germany.
SELECT
    ROUND(CAST(SUM(store_revenue.transaction_value)AS numeric),2) AS total_sales , dim_store_details.store_type, dim_store_details.country_code 
FROM
    (SELECT
        store_code, orders_table.product_code, (product_quantity * product_price) AS "transaction_value"
    FROM
        orders_table
    JOIN
        dim_products ON orders_table.product_code = dim_products.product_code) AS store_revenue
JOIN
    dim_store_details ON store_revenue.store_code = dim_store_details.store_code
WHERE
    dim_store_details.country_code = 'DE'
GROUP BY
    dim_store_details.store_type, dim_store_details.country_code
ORDER BY
    total_sales ASC;

-- Task 9: Determine the average time taken between each sale grouped by year.
WITH 
    join_table AS (
        SELECT
            year, month, day, timestamp, orders_table.date_uuid,
            CAST(concat(year, '-', month, '-', day, ' ', timestamp) as TIMESTAMP) AS combined_timestamp
        FROM
            orders_table
        JOIN
            dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid),

    lead_table AS (
        SELECT
            join_table.combined_timestamp, LEAD(combined_timestamp,1) OVER(ORDER BY combined_timestamp) AS Lead_time, date_uuid, year
        FROM
            join_table
        ORDER BY
            year, combined_timestamp, timestamp)
SELECT 
    year, AVG(lead_time - combined_timestamp) AS actual_time_taken
FROM
    lead_table
GROUP BY
    year
ORDER BY
    actual_time_taken DESC
LIMIT
    5;