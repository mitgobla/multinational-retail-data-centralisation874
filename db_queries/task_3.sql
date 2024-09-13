-- Q: Which months produced the largest amount of sales?

SELECT
	date_part('month', dim_date_times.datetime) AS "month",
	ROUND(CAST(SUM(dim_products.product_price * orders_table.product_quantity) AS NUMERIC), 2) AS total_sales
FROM public.orders_table
INNER JOIN public.dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN public.dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY "month"
ORDER BY total_sales DESC;