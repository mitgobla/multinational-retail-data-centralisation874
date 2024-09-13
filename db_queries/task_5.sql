-- Q: What percentage of sales come through each type of store?
-- Get total sales for each store type, by grouping the store type
-- Since orders also has quantity we must multiply the price by this quantity to get total sale in order
WITH total_sales_by_store_cte AS (
    SELECT
        dim_store_details.store_type,
        ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::DECIMAL, 2) AS total_sales
    FROM public.orders_table
    INNER JOIN public.dim_products ON dim_products.product_code = orders_table.product_code
    INNER JOIN public.dim_store_details ON dim_store_details.store_code = orders_table.store_code
    GROUP BY dim_store_details.store_type
)
-- Using the CTE we can sum the total sales to use in percentage calculation
SELECT
    store_type,
    total_sales,
    ROUND(total_sales * 100.0 / (SELECT SUM(total_sales) FROM total_sales_by_store_cte), 2) AS percentage_total
FROM total_sales_by_store_cte
ORDER BY percentage_total DESC;