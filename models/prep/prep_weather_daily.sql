WITH daily_data AS (
        SELECT * 
        FROM {{ref('staging_weather_daily')}}
    ),
    add_features AS (
        SELECT *
    		, EXTRACT(DAY FROM date) AS date_day -- number of the day of month
    		, EXTRACT(MONTH FROM date) AS date_month 	-- number of the month of year
    		, EXTRACT(YEAR FROM date) AS date_year 		-- number of year
    		, EXTRACT(WEEK FROM date) AS cw 			-- number of the week of year
    		, to_char(date, 'Month') AS month_name 	-- name of the month
    		, to_char(date, 'Day') AS weekday 		-- name of the weekday
        FROM daily_data 
    ),
    add_more_features AS (
        SELECT *
    		, (CASE 
    			WHEN month_name in ('November', 'December', 'January', 'February') THEN 'winter'
    			WHEN ('March', 'April', 'May') THEN 'spring'
                WHEN ('June', 'July', 'August') THEN 'summer'
                WHEN ('September', 'October') THEN 'autumn'
    		END) AS season
        FROM add_features
    )
    SELECT *
    FROM add_more_features
    ORDER BY date