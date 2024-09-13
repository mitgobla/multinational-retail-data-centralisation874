-- Q: How many stores does the business have and in which countries?

SELECT
	country_code AS country,
	COUNT(country_code) AS total_no_stores
FROM dim_store_details
WHERE store_type != 'Web Portal'
GROUP BY country
ORDER BY total_no_stores DESC;