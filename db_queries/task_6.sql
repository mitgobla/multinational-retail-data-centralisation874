-- Q: Which month in each year produced the highest cost of sales?
SELECT
	ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::DECIMAL, 2) AS total_sales,
	date_part('year', dim_date_times.datetime) AS "year",
	date_part('month', dim_date_times.datetime) AS "month"
FROM public.orders_table
INNER JOIN public.dim_date_times ON dim_date_times.date_uuid = orders_table.date_uuid
INNER JOIN public.dim_products ON dim_products.product_code = orders_table.product_code
GROUP BY "year", "month"
ORDER BY total_sales DESC;
