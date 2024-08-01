import argparse
import sqlite3
import pandas as pd
from pandasql import sqldf


def write_data(df, db_path, table_name):
    """
    Write data to a SQLite database table.
    Parameters
    ----------
    df : pd.DataFrame
        Data to be written to the table.
    db_path : str
        Path to the SQLite database file.
    table_name : str
        Name of the table to write the data to.
    """
    # Create a connection to the database
    conn = sqlite3.connect(db_path)
    # Write the data to the table
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    # Close the connection
    conn.close()



def read_data(db_path, table_name):
    """
    Read data from a SQLite database table.
    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.
    table_name : str
        Name of the table to read the data from.
    Returns
    -------
    pd.DataFrame
        Data read from the table.
    """
    # Create a connection to the database
    conn = sqlite3.connect(db_path)
    # Read the data from the table
    df = pd.read_sql(f'SELECT * FROM {table_name}', conn)
    # Close the connection
    conn.close()
    return df

def transform_data(df):
    """
    Transform the data by calculating the number of interactions per user and per product.
    Add a new column `interaction_count` to the data for each user and product.
    Parameters
    ----------
    df : pd.DataFrame
        Data to be transformed.
    Returns
    -------
    pd.DataFrame
        Transformed data.
    """
    # Calculate the number of interactions per user and per product using pandas sql
    interactions_count = sqldf("""
                  SELECT D.*,interaction_counts.interaction_count as interaction_count from 
                  df as D inner join
                  (  
                    SELECT 
                        user_id,
                        product_id,
                        count(*) interaction_count 
                    FROM df 
                    GROUP BY 
                        user_id, 
                        product_id
                    ) as interaction_counts
                    on D.user_id = interaction_counts.user_id and D.product_id = interaction_counts.product_id
                """) 
    #Total number of interactions per day.
    interactions_per_day = sqldf("""
                                 SELECT DATE(timestamp),count(*) as interaction_count from df group by DATE(timestamp)
                                    """)
    #Top 5 users by the number of interactions.
    top_5_users = sqldf("""
                        SELECT user_id,count(*) as interaction_count from df group by user_id order by interaction_count desc limit 5
                        """)
    return interactions_count,interactions_per_day,top_5_users 

def transform(cleaned_db_path, cleaned_table_name, transformed_db_path, transformed_table_name):
    """
    Transform the data by calculating the number of interactions per user and per product.
    Add a new column `interaction_count` to the data for each user and product.
    Parameters
    ----------
    cleaned_db_path : str
        Path to the SQLite database file containing the cleaned data.
    cleaned_table_name : str
        Name of the table containing the cleaned data.
    transformed_db_path : str
        Path to the SQLite database file where the transformed data should be stored.
    transformed_table_name : str
        Name of the table where the transformed data should be stored.
    """
    # Read the cleaned data from the database
    cleaned_data = read_data(cleaned_db_path, cleaned_table_name)
    # Transform the data
    interactions_count,interactions_per_day,top_5_users  = transform_data(cleaned_data)
    # Write the transformed data to the database
    write_data(cleaned_data, transformed_db_path, transformed_table_name)
    write_data(interactions_count, transformed_db_path, 'interactions_count')
    write_data(interactions_per_day, transformed_db_path, 'interactions_per_day')
    write_data(top_5_users, transformed_db_path, 'top_5_users')


#python3 transform.py --clean_db_path ../clean/raw_db.db --clean_table_name raw_interaction --transformed_db_path pub_db.db --transformed_table_name pub_interaction 
if __name__ == "__main__":
    #params 
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--clean_db_path", type=str,
                        help="path to file from where we should read the data")
    parser.add_argument("--clean_table_name", type=str,
                        help="table name from where we should read the data")
    parser.add_argument("--transformed_db_path", type=str,
                        help="path to file where we should save the cleaned data")
    parser.add_argument("--transformed_table_name", type=str,
                        help="table name where we should save the cleaned data")
    args = parser.parse_args() 
    cleaned_db_path = str(args.clean_db_path)
    cleaned_table_name = str(args.clean_table_name)
    transformed_db_path = str(args.transformed_db_path)
    transformed_table_name = str(args.transformed_table_name)
    transform(cleaned_db_path, cleaned_table_name, transformed_db_path, transformed_table_name)
