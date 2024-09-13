-- Q: How quickly is the company making sales?
-- Partition by the year so we can get the next datetime from the current for the same year
WITH next_date_cte AS (
	SELECT
		datetime,
		date_part('year', datetime) AS "year",
		LEAD(datetime) OVER (
			PARTITION BY date_part('year', datetime)
			ORDER BY datetime
		) AS next_sale_date
	FROM dim_date_times
)
SELECT
	"year",
	AVG(next_sale_date - datetime) AS actual_time_taken
FROM next_date_cte
WHERE next_sale_date IS NOT NULL
GROUP BY "year"
ORDER BY actual_time_taken DESC;