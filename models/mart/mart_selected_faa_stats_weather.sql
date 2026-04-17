/* In a table `mart_selected_faa_stats_weather.sql` we want to see **for each airport daily**:

- only the airports we collected the weather data for
- unique number of departures connections
- unique number of arrival connections
- how many flight were planned in total (departures & arrivals)
- how many flights were canceled in total (departures & arrivals)
- how many flights were diverted in total (departures & arrivals)
- how many flights actually occured in total (departures & arrivals)
- *(optional) how many unique airplanes travelled on average*
- *(optional) how many unique airlines were in service  on average* 
- (optional) add city, country and name of the airport
- daily min temperature
- daily max temperature
- daily precipitation 
- daily snow fall
- daily average wind direction 
- daily average wind speed
- daily wnd peakgust */


WITH departures AS (
		-- airport as departure
		SELECT origin,
		flight_date,
			COUNT(DISTINCT dest) AS nunique_to,
			COUNT(sched_dep_time) AS dep_planned,
			SUM(cancelled) AS dep_cancelled,
			SUM(diverted) AS dep_diverted,
		--	COUNT(sched_dep_time) - SUM(cancelled) AS dep_n_flights_calc,
			SUM(CASE WHEN cancelled = 0 THEN 1 END) AS dep_n_flights_calc,
			COUNT(DISTINCT tail_number) AS nunique_tail_number_from,
			COUNT(DISTINCT airline) AS nunique_airline_from
		FROM {{ref('prep_flights')}}
		WHERE origin IN ('JFK', 'MIA', 'LAX')
		GROUP BY origin, flight_date
),
arrivals AS(
		-- airport as destination
		SELECT dest, 
		flight_date,
			COUNT(DISTINCT origin) AS nunique_from,
			COUNT(sched_arr_time) AS arr_planned,
			SUM(cancelled) AS arr_cancelled,
			SUM(diverted) AS arr_diverted,
			--	COUNT(sched_arr_time) - SUM(cancelled) AS arr_n_flights_calc,
			SUM(CASE WHEN cancelled = 0 THEN 1 END) AS arr_n_flights_calc,
			COUNT(DISTINCT tail_number) AS nunique_tail_number_to,
			COUNT(DISTINCT airline) AS nunique_airline_to
		FROM {{ref('prep_flights')}}
		WHERE dest IN ('JFK', 'MIA', 'LAX')
		GROUP BY dest, flight_date
),
total_stats AS (
		SELECT origin AS airport_code,
		d.flight_date AS date,
				nunique_to,
				nunique_from,
				d.dep_planned + a.arr_planned AS total_planned,
				d.dep_cancelled + a.arr_cancelled AS total_cancelled,
				d.dep_diverted + a.arr_diverted AS total_diverted,
				d.dep_n_flights_calc + a.arr_n_flights_calc AS total_n_flights_calc
		FROM departures d
		JOIN arrivals a
		ON d.origin = a.dest AND d.flight_date = a.flight_date 
)
SELECT ap.name, ap.city, ap.country, 
		ts.*, 
		swd.min_temp_c, swd.max_temp_c, swd.precipitation_mm, swd.max_snow_mm, swd.avg_wind_direction,
		swd.avg_wind_speed_kmh, swd.wind_peakgust_kmh
FROM total_stats ts
JOIN {{ref('prep_airports')}} ap
ON ts.airport_code = ap.faa
JOIN {{ref('staging_weather_daily')}} swd
ON ts.date = swd.date AND ts.airport_code = swd.airport_code
