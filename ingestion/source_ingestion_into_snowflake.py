import pandas as pd
from database_connection import DatabaseConnection
from datetime import date


def parse_dates(df, col):
    df[col] = pd.to_datetime(df[col], format='%m/%d/%Y %H:%M', errors='coerce')
    df[col] = df[col].dt.tz_localize(None)
    return df  # keep as datetime64 for filtering

def load_source_with_date_filter(from_dt, to_dt)-> pd.DataFrame:
    raw_data = pd.read_csv('../data/hashtag_donaldtrump.csv', low_memory=False)
    raw_data.columns = raw_data.columns.str.lower().str.strip()

    raw_data = parse_dates(raw_data, 'created_at')
    raw_data = parse_dates(raw_data, 'user_join_date')
    raw_data = raw_data.drop(columns=['collected_at'])

    # Filter while still datetime64
    from_ts = pd.Timestamp(from_dt)
    to_ts   = pd.Timestamp(to_dt)
    raw_data = raw_data[
        (raw_data['created_at'] >= from_ts) &
        (raw_data['created_at'] <= to_ts)
    ]

    # AFTER filtering, convert to date string for Snowflake DATE column
    raw_data['created_at']   = raw_data['created_at'].dt.strftime('%Y-%m-%d')
    raw_data['user_join_date'] = raw_data['user_join_date'].dt.strftime('%Y-%m-%d')

    return raw_data


def store_filtered_data_into_db(filterer_date: pd.DataFrame , db_conn: DatabaseConnection) -> dict:
    return  db_conn.load_dataframe_into_db(filterer_date , "TWEETS")

def data_ingestion(db_conn: DatabaseConnection):
    
    filtered_data = load_source_with_date_filter(date(2020,1,1),date(2020,10,30))
    
    inserted_count =  db_conn.load_dataframe_into_db(filtered_data.head(5), "TWEETS")
    raw_data_count = len(filtered_data)
    return {
        "to_load" : raw_data_count,
        "inserted"  : inserted_count,
        "status"    : "Success" if inserted_count== raw_data_count else "Failed"
    }

    





if __name__ == '__main__':
    data_ingestion(DatabaseConnection())