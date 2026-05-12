{{config(schema='tweets_mart' , materialized='incremental') }}



WITH fill_user_data as (

    SELECT 
        user_id,
        user_name,
        user_join_date,
        user_screen_name,
        user_description,
        user_followers_count,
    FROM {{ref('stg_tweets')}}





    
)

SELECT *
FROM fill_user_data

{% if is_incremental() %}
  WHERE user_id NOT IN (SELECT user_id FROM {{ this }})
{% endif %}
