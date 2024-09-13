-- Q: Which German store type is selling the most?
SELECT
	ROUND(SUM(dim_products.product_price * orders_table.product_quantity)::DECIMAL, 2) AS total_sales,
	dim_store_details.store_type,
	dim_store_details.country_code
FROM public.orders_table
INNER JOIN public.dim_store_details ON dim_store_details.store_code = orders_table.store_code
INNER JOIN public.dim_products ON dim_products.product_code = orders_table.product_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY total_sales;