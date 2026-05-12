{{config(schema='STAGING')}}




WITH raw_tweets as (

    SELECT *
    FROM {{ref('tweets_raw')}}
),

drop_retweets as (

    SELECT * , 
    FROM raw_tweets
    WHERE tweet NOT LIKE '@RT'

),
drop_empty_tweets as (

    SELECT *
    FROM drop_retweets
    WHERE (tweet IS NOT NULL  AND tweet_id IS NOT NULL ) 
),

add_raw_numbers_per_tweet_id as (

    SELECT * ,
           row_number() over(partition by tweet_id, tweet order by tweet_id) as rn 
    FROM drop_empty_tweets
),

drop_duplicats_tweets as (

    SELECT * exclude(rn)
    FROM add_raw_numbers_per_tweet_id
    WHERE rn = 1

)

SELECT * FROM drop_duplicats_tweets

