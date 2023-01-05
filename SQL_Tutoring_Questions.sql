--------------------------------------------------------------------------------------------------------------------------------------------------------
-- GROUP BY AND HAVING
--------------------------------------------------------------------------------------------------------------------------------------------------------
/*
- What month has the highest liquor sales of 12 packs?
    - Your boss has decided that they also want to see total and average volume sold in gallons.
    - They have also decided that they only want to see months with an average sales in dollars above $140.
    - Dataset: iowa_liquor_sales ; Table: sales
*/

SELECT
  EXTRACT(MONTH FROM date) AS month
  , SUM(sale_dollars) AS totSales
  , ROUND(AVG(sale_dollars),2) AS avgSales
  , SUM(volume_sold_gallons) AS totVolGal
  , ROUND(AVG(volume_sold_gallons),2) AS avgVolGal
  -- , SUM(volume_sold_liters) AS totVolLiter
  -- , ROUND(AVG(volume_sold_liters), 2) AS avgVolLiter
FROM
  `bigquery-public-data.iowa_liquor_sales.sales`
WHERE
  pack = 12
GROUP BY
  month
HAVING 
  avgSales >= 140
ORDER BY
  totSales DESC
  , totVolGal DESC

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- CASE, In-line view, and One-Hot Encoding
--------------------------------------------------------------------------------------------------------------------------------------------------------
/*

- How many times has each country had an earthquake where the total deaths was more than the global average?
    - Inlcude Times where each country has had higher missing and injuries totals than the global average
    - Feature Engineer flags to find the number of occurences
    - Alias high death occurences as "High Death", high missing occurances as 'High Missing' and high injury occurences as 'High Injury'
    - Sort results by High Death in descending order
    - Dataset: noaa_significant_earthquakes, Table: earthquakes

    - PT 2: What countries have more high injury occurences than high death occurences?

*/
SELECT 
  featEng.country
  , SUM(featEng.high_death_flag) AS High_Deaths
  , SUM(featEng.high_missing_flag) AS High_Missing
  , SUM(featEng.high_injuries_flag) AS High_Injury

FROM 
 
  (SELECT
    year
    , country

    -- CASE STATEMENT FOR HIGH_DEATH_FLAG
    , CASE 
        WHEN SUM(total_deaths) > (SELECT ROUND(AVG(total_deaths),2) FROM `bigquery-public-data.noaa_significant_earthquakes.earthquakes`) THEN 1
        ELSE 0
      END AS High_death_flag

    -- CASE STATEMENT FOR HIGH_MISSING_FLAG
    , CASE 
        WHEN SUM(total_missing) > (SELECT ROUND(AVG(missing),2) FROM `bigquery-public-data.noaa_significant_earthquakes.earthquakes`) THEN 1
        ELSE 0
      END AS High_missing_flag

    -- CASE STATEMENT FOR HIGH_INJURIES FLAG
    , CASE 
        WHEN SUM(total_injuries) > (SELECT ROUND(AVG(total_injuries),2) FROM `bigquery-public-data.noaa_significant_earthquakes.earthquakes`) THEN 1
        ELSE 0
      END AS High_injuries_flag


  FROM 
    `bigquery-public-data.noaa_significant_earthquakes.earthquakes`
  GROUP BY
    year 
    , country
  ) AS featEng 

GROUP BY 
        featEng.country

-- HAVING CLAUSE IS ONLY NECESSARY FOR PT. 2 OF THIS QUESTION
HAVING 
  High_Injury > High_Deaths
ORDER BY 
  High_Deaths DESC

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Coalesce, In-Line Views, Roll-Up, and Feature Engineering
--------------------------------------------------------------------------------------------------------------------------------------------------------


-- This Dataset is 4.58 GB big so we're just gonna take a slice. Create a table called "nyc_citibike_trips" in the dataset "mydb".
--Copy and run this code in your BQ editor:
CREATE OR REPLACE TABLE 
  `mydb.nyc_citibike_trips`
  AS
SELECT 
  start_station_name
  , start_station_id
  , end_station_name
  , end_station_id
  , gender
  , birth_year
  , tripduration
FROM 
  `bigquery-public-data.new_york.citibike_trips`
;
/*

- How many one-way citibike trips have there been in New York City?
  - Include subtotals for each bike route (formatted like: start_station_name -> end_station_name)
  - Inlcude the average age of the rider and the average trip duration in minutes (look up CURRENT_DATE())
  - Filter out any results where the starting location is the same as the ending location
  - Only include riders that are male or female and are at most 40 years old
  - Make sure the grand total is labeld "GRAND TOTAL"
  - Sort results totTrips descending 

*/
SELECT
  COALESCE(trips.route, 'Grand Total') AS route_name
  , COUNT(trips.tripduration) AS totTrips
  , ROUND(AVG(age),2) AS avgAge
  , ROUND(AVG(trips.duration_minutes), 2) AS avgTripDurationMinutes
FROM

  -- IN-LINE VIEW FOR ROUTE_NAME
  (SELECT
    start_station_name
    , end_station_name
    , gender
    , EXTRACT(YEAR FROM CURRENT_DATE()) - birth_year AS Age
    , tripduration
    , CONCAT(start_station_name, ' ', '-->', ' ', end_station_name) AS route
    , ROUND(tripduration/60,2) AS duration_minutes
  FROM
  `elite-totality-301818.mydb.nyc_citibike_trips`
  ) AS trips
WHERE
  trips.age <= 40
  AND
  trips.gender IN ('male', 'female')
  AND
  -- NEEDED TO FILTER OUT ANY ROUTES WHERE STARTING LOCATION = ENDING LOCATION
  trips.start_station_name != trips.end_station_name
GROUP BY
  ROLLUP
  (
  trips.route
  )
ORDER BY
  totTrips DESC

;

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Views, Coalesce, Roll-Up, Case Statements, One-Hot Encoding
--------------------------------------------------------------------------------------------------------------------------------------------------------

/*
- You've been asked to collect data on Earthquakes across the Globe from 1800 and on. They want the following information:
    - year & country
    - The following continents:
        - South America: Argentina, Bolivia, Brazil, Chile, Colombia, Ecuador, Guyana, Paraguay, Peru, Suriname, Uruguay, Venezuela
        - North America: Canada, USA, Mexico, Nicaragua, Honduras, Cuba, Guatemala, Panama, Costa Rica, Dominican Republic, Haiti, Belize
        - Europe: Russia, Turkey, Gernmany, France, United Kingdom, Itlay, Spain, Ukraine, Poland, Romania, Kazakhstan, Netherlands
    - Total number of earthquakes, tsunamis, deaths (use total_deaths), injuries (use total_injuries)
    - Total number of high death and injury instances (when the total is larger then the overall average)
    
    - Make sure to include the subtotals for continents and countries
    - For NULL values in the contintent column replace it with "GRAND TOTAL"
    - For NULL values in the country column replace it with "CONTINENT TOTAL" 

*/
CREATE OR REPLACE VIEW
  `elite-totality-301818.mydb.eqChallenge`
AS

SELECT
  year
  , country
  , COUNT(id) as EQs
  , total_deaths
  , total_injuries
 
 -- CASE STATEMENT FOR TSUNAMI FLAG
  , CASE
      WHEN flag_tsunami IS NOT NULL THEN 1
      ELSE NULL
    END AS Tsunami_flag
 
  -- CASE STATEMENT FOR CONTINENTS
  , CASE
      WHEN country IN ('ARGENTINA','BOLIVIA','BRAZIL','CHILE','COLOMBIA','ECUADOR','GUYANA','PARAGUAY','PERU','SURINAME','URUGUAY','VENEZUELA') THEN 'South America'
      WHEN country IN ('CANADA','USA','MEXICO','NICARAGUA','HONDURAS','CUBA','GUATEMALA','PANAMA', 'COSTA RICA','DOMINICAN REPUBLIC', 'HAITI','BELIZE') THEN 'North America'
      WHEN country IN ('RUSSIA','TURKEY','GERMANY','FRANCE','UNITED KINGDOM','ITALY','SPAIN','UKRAINE','POLAND','ROMANIA','KAZAKHSTAN','NETHERLANDS') THEN 'Europe'
      ELSE 'Other'
    END AS continent

  -- CASE STATEMENT FOR HIGH_DEATH_FLAG
  , CASE 
      WHEN total_deaths IS NULL THEN NULL
      WHEN SUM(total_deaths) > (SELECT ROUND(AVG(total_deaths),2) FROM `bigquery-public-data.noaa_significant_earthquakes.earthquakes`) THEN 1
      ELSE 0
    END AS High_death_flag
  
  -- CASE STATEMENT FOR HIGH_INJURIES FLAG
  , CASE 
      WHEN total_injuries IS NULL THEN NULL 
      WHEN SUM(total_injuries) > (SELECT ROUND(AVG(total_injuries),2) FROM `bigquery-public-data.noaa_significant_earthquakes.earthquakes`) THEN 1
      ELSE 0
    END AS High_injuries_flag

FROM 
  `bigquery-public-data.noaa_significant_earthquakes.earthquakes`
GROUP BY
  year
  , country
  , state
  , flag_tsunami
  , total_deaths
  , total_injuries

-- ;
-- SELECT *
-- FROM `elite-totality-301818.mydb.eqChallenge`

;
/*

- How many total earthquakes where have there been in our "eqChallenge" view?
    - Include subtotals for each continent and countries
    - Inlcude total tsunamis triggered, total high death instances, total deaths, averge deaths, total high injury instances, toal injuries, average injuries

*/
SELECT
  COALESCE(continent,'GRAND TOTAL') AS continents
  , COALESCE(country,'CONTINENT TOTAL') AS countries 
  , SUM(EQs) AS totalEQs
  , SUM(Tsunami_flag) AS totTsunami
  , SUM(High_death_flag) AS totHighDeath
  , SUM(total_deaths) AS totDeath 
  , ROUND(AVG(total_deaths),2) AS avgDeaths
  , SUM(High_injuries_flag) AS totHighInjuries
  , SUM(total_injuries) AS totInjuries
  , ROUND(AVG(total_deaths),2) AS avgInjuries
FROM
  `elite-totality-301818.mydb.eqChallenge`
WHERE
  year >= 1800
GROUP BY 
  ROLLUP(continent,country)
ORDER BY
  totalEQs DESC,
  continent

;
