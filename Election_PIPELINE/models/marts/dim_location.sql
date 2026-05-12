{{config(schema='tweets_mart' ,materialized='incremental', unique_key = ['city' , 'state','country'] ) }}


with fill_location_dim as (

    SELECT 
        uuid_string() as location_id,
        CITY,
        country,
        continent,
        lat,
        long,
        state,
        state_code
    FROM {{ref('stg_tweets')}}


)

SELECT *
FROM fill_location_dim