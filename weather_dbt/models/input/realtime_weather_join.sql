SELECT
    r.CITY_ID,
    c."Name" AS CITY_NAME,
    c."Latitude" AS CITY_LATITUDE,
    c."Longitude" AS CITY_LONGITUDE,
    r.DATE_TIME,
    ((r.TEMP - 273.15) * 9/5 + 32) AS TEMP_FAHRENHEIT,
    ((r.FEELS_LIKE - 273.15) * 9/5 + 32) AS FEELS_LIKE_FAHRENHEIT,
    r.PRESSURE,
    r.HUMIDITY,
    r.WIND_SPEED,
    r.CLOUD_COVERAGE,
    r.WEATHER_MAIN,
    r.WEATHER_DET
FROM
    {{ source('openweather', 'WEATHER_REALTIME_TABLE') }} r
JOIN
    {{ source('openweather', 'CITY_DIMENSION_TABLE') }} c
ON
    r.CITY_ID = c.CITY_ID