WITH hourly_data AS (
        SELECT * 
        FROM {{ref('staging_weather_hourly')}}
    ),
    add_features AS (
        SELECT *
    		, timestamp::DATE AS date               -- only date (hours:minutes:seconds) as DATE data type
    		, timestamp::TIME AS time                           -- only time (hours:minutes:seconds) as TIME data type
            , TO_CHAR(timestamp,'HH24:MI') as hour  -- time (hours:minutes) as TEXT data type
            , TO_CHAR(timestamp, 'FMmonth') AS month_name   -- month name as a TEXT
            , TO_CHAR(date, 'Day') AS weekday  AS weekday        -- weekday name as TEXT        
            , DATE_PART('day', timestamp) AS date_day
            , EXTRACT(MONTH FROM date) AS date_month 	-- number of the month of year
    		, EXTRACT(YEAR FROM date) AS date_year 		-- number of year
    		, EXTRACT(WEEK FROM date) AS cw 			-- number of the week of year
        FROM hourly_data
    ),
    add_more_features AS (
        SELECT *
    		,(CASE 
    			WHEN time BETWEEN ... AND ... THEN 'night'
    			WHEN ... THEN 'day'
    			WHEN ... THEN 'evening'
    		END) AS day_part
        FROM add_features
    )
    
    SELECT *
    FROM add_more_features