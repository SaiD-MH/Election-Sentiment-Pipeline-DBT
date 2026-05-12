{{ config(schema='RAW') }}


with cte as (


    SELECT * 
    FROM {{source('RAW_TWEETS' , 'TWEETS')}}
)


SELECT *
FROM cte