-- Step 6: Create Dimension and Fact Tables

-- Create Dimension Tables

CREATE TABLE sakila.dim_store AS
SELECT
  store_id,
  manager_staff_id,
  address_id,
  last_update
FROM sakila.store;

CREATE TABLE sakila.dim_staff AS
SELECT
  staff_id,
  first_name,
  last_name,
  address_id,
  picture,
  email,
  store_id,
  active,
  username,
  password,
  last_update
FROM sakila.staff;

CREATE TABLE sakila.dim_customer AS
SELECT
  customer_id,
  store_id,
  first_name,
  last_name,
  email,
  address_id,
  active,
  create_date,
  last_update
FROM sakila.customer;

CREATE TABLE sakila.dim_film AS
SELECT
  film_id,
  title,
  description,
  release_year,
  language_id,
  original_language_id,
  rental_duration,
  rental_rate,
  length,
  replacement_cost,
  rating,
  special_features,
  last_update
FROM sakila.film;

-- Create Fact Table

CREATE TABLE sakila.fact_payment AS
SELECT
  p.payment_id,
  p.customer_id,
  p.staff_id,
  p.rental_id,
  p.amount,
  p.payment_date,
  p.last_update,
  i.inventory_id,
  i.film_id,
  i.store_id
FROM sakila.payment p
JOIN sakila.inventory i ON p.rental_id = i.inventory_id;

-- Step 7: Write ETL Script

-- List the total revenue of each store everyday.
SELECT
  f.store_id,
  f.payment_date,
  SUM(f.amount) AS total_revenue
FROM sakila.fact_payment f
GROUP BY f.store_id, f.payment_date
ORDER BY f.store_id, f.payment_date;

-- List the total revenue of totally everyday.
SELECT
  f.payment_date,
  SUM(f.amount) AS total_revenue
FROM sakila.fact_payment f
GROUP BY f.payment_date
ORDER BY f.payment_date;

-- List the top store according to their weekly revenue every week.
SELECT
  f.store_id,
  DATE_TRUNC('week', f.payment_date) AS week_start,
  SUM(f.amount) AS weekly_revenue
FROM sakila.fact_payment f
GROUP BY f.store_id, week_start
ORDER BY f.store_id, week_start;

-- List top sales clerk who have the most sales each day/week/month.
SELECT
  f.staff_id,
  f.payment_date,
  SUM(f.amount) AS total_sales
FROM sakila.fact_payment f
GROUP BY f.staff_id, f.payment_date
ORDER BY f.staff_id, f.payment_date;

-- Which film is the top film each week/month in each store/totally?
SELECT
  f.store_id,
  f.film_id,
  DATE_TRUNC('week', f.payment_date) AS week_start,
  SUM(f.amount) AS weekly_revenue
FROM sakila.fact_payment f
GROUP BY f.store_id, f.film_id, week_start
ORDER BY f.store_id, f.film_id, week_start;

-- Who are our top 10 customers each month/year?
SELECT
  f.customer_id,
  DATE_TRUNC('month', f.payment_date) AS month_start,
  SUM(f.amount) AS total_spent
FROM sakila.fact_payment f
GROUP BY f.customer_id, month_start
ORDER BY total_spent DESC
LIMIT 10;

-- Is there any store the sales are in a decline trend (within the recent 4 weeks, the avg sales of each week is declining)
WITH WeeklyAvg AS (
  SELECT
    store_id,
    DATE_TRUNC('week', payment_date) AS week_start,
    AVG(amount) AS avg_weekly_sales
  FROM sakila.fact_payment
  GROUP BY store_id, week_start
),
TrendAnalysis AS (
  SELECT
    store_id,
    week_start,
    LAG(avg_weekly_sales) OVER (PARTITION BY store_id ORDER BY week_start) AS prev_avg_weekly_sales,
    avg_weekly_sales
  FROM WeeklyAvg
  WHERE week_start >= DATEADD('week', -4, CURRENT_DATE()) -- Adjust as needed
)
SELECT
  store_id,
  week_start
FROM TrendAnalysis
WHERE prev_avg_weekly_sales IS NOT NULL AND avg_weekly_sales < prev_avg_weekly_sales;
