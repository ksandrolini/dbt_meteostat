/*
In a table `mart_route_stats.sql` we want to see **for each route over all time**:

- origin airport code
- destination airport code 
- total flights on this route
- unique airplanes
- unique airlines
- on average what is the actual elapsed time
- on average what is the delay on arrival
- what was the max delay?
- what was the min delay?
- total number of cancelled 
- total number of diverted
- add city, country and name for both, origin and destination, airports

*/

-- SELECT * FROM flights LIMIT 5

WITH routes AS (
		-- airport as departure
		SELECT origin,
			dest,
			COUNT(*) AS total_flights,
			COUNT(DISTINCT tail_number) AS unique_planes,
			COUNT(DISTINCT airline) AS unique_airlines,
			ROUND(AVG(actual_elapsed_time),2) AS avg_elapsed_time,
			ROUND(AVG(arr_delay),2) AS avg_arrival_delay,
			MAX(arr_delay) AS max_delay,
			MIN(arr_delay) AS min_delay,
			SUM(cancelled) AS total_cancelled,
			SUM(diverted) AS total_diverted
			FROM {{ref('prep_flights')}}
		GROUP BY origin, dest
)
SELECT ap.name AS origin_name, ap.city AS origin_city, ap.country AS origin_country, 
		ap2.name AS dest_name, ap2.city AS dest_city, ap2.country AS dest_country, 
		r.*
FROM routes r
JOIN {{ref('prep_airports')}} ap
ON r.origin = ap.faa
JOIN {{ref('prep_airports')}} ap2
ON r.dest = ap2.faa
