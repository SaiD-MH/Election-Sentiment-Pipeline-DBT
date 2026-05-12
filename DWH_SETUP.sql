CREATE OR REPLACE WAREHOUSE election_wh 
WITH WAREHOUSE_SIZE = 'XSMALL' 
AUTO_SUSPEND = 60 
AUTO_RESUME = TRUE 
INITIALLY_SUSPENDED = TRUE; 

CREATE OR REPLACE DATABASE election_db; 

CREATE OR REPLACE SCHEMA RAW;



CREATE USER dbt_user PASSWORD = 'dbt_password' 

LOGIN_NAME = 'dbt_user' 

DEFAULT_ROLE = ACCOUNTADMIN 

MUST_CHANGE_PASSWORD = FALSE; 

GRANT ROLE ACCOUNTADMIN TO USER dbt_user; 

GRANT USAGE ON DATABASE election_db TO ROLE ACCOUNTADMIN; 

GRANT USAGE ON SCHEMA election_db.raw TO ROLE ACCOUNTADMIN; 

GRANT USAGE ON WAREHOUSE election_wh TO ROLE ACCOUNTADMIN; 

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA election_db.raw TO ROLE ACCOUNTADMIN; 


select lower(current_region()) , lower(current_account());



CREATE TABLE ELECTION_DB.RAW.TWEETS (
    id                   NUMBER AUTOINCREMENT PRIMARY KEY,
    created_at           TIMESTAMP,
    tweet_id             BIGINT,
    tweet                TEXT,
    likes                INTEGER          DEFAULT 0,
    retweet_count        INTEGER          DEFAULT 0,
    source               VARCHAR(255),
    user_id              BIGINT,
    user_name            VARCHAR(255),
    user_screen_name     VARCHAR(255),
    user_description     TEXT,
    user_join_date       TIMESTAMP,
    user_followers_count INTEGER,
    user_location        VARCHAR(255),
    lat                  DECIMAL(9, 6),
    long                 DECIMAL(9, 6),
    city                 VARCHAR(100),
    country              VARCHAR(100),
    continent            VARCHAR(100),
    state                VARCHAR(100),
    state_code           VARCHAR(10),
    collected_at         TIMESTAMP
);