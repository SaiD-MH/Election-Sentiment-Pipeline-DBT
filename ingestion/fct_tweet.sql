-- {{config(schema='tweets_mart' , materialized='incremental') }}




-- WITH dim_date as (
--     SELECT *
--     FROM {{ ref('dim_date') }}
-- ),
-- dim_location as (
--     SELECT *
--     FROM {{ ref('dim_location') }}
-- ),
-- dim_user as (
--     SELECT *
--     FROM {{ ref('dim_user') }}
-- ),

-- fill_fct_tweets as (

--     SELECT
--     tweet_id,
--     tweet,
--     likes,
--     retweet_count,
--     source,
--     collected_at,
--     created_at,
--     usr.user_id,
--     dt.date_key,
--     loc.location_id

--     FROM {{ ref('stg_tweets') }} stg
--     LEFT JOIN dim_user usr 
--     USING (user_id)

--     LEFT JOIN dim_date dt
--     ON CAST(created_at to int) = dt.date_key
-- )

-- SELECT *
-- FROM fill_fct_tweets