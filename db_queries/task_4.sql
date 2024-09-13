-- Q: How many sales are coming from online?

SELECT
	COUNT(product_code) AS number_of_sales,
	SUM(product_quantity) AS product_quantity_count,
	CASE
		WHEN store_code LIKE 'WEB%' THEN 'Web'
		ELSE 'Offline'
	END AS "location"
FROM public.orders_table
GROUP BY "location";