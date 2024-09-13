-- Q: Which locations currently have the most stores?

SELECT
	locality,
	COUNT(locality) as total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC;