WITH validation AS (
    SELECT
        {{ column }} AS birthday_date
    FROM {{ model }}
    WHERE {{ column }} < '1924-01-01'  -- Before 1924
       OR {{ column }} > DATEADD(YEAR, -16, current_date)  -- After 16 years from the current date
)

SELECT *
FROM validation