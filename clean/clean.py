import argparse
import sqlite3
import pandas as pd


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

def clean_data(df):
    """
    Clean the data by handling missing values and ensuring correct data types.
    Parameters
    ----------
    df : pd.DataFrame
        Data to be cleaned.
    Returns
    -------
    pd.DataFrame
        Cleaned data.
    """
    # Handle missing values
    df = df.dropna()
    # Ensure correct data types
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

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

def clean(ingested_db_path, ingested_table_name, cleaned_db_path, cleaned_table_name):
    """
    Clean the ingested data and write the cleaned data to a new table.
    Parameters
    ----------
    ingested_db_path : str
        Path to the SQLite database file with the ingested data.
    ingested_table_name : str
        Name of the table with the ingested data.
    cleaned_db_path : str
        Path to the SQLite database file where the cleaned data should be stored.
    cleaned_table_name : str
        Name of the table where the cleaned data should be stored.
    """
    # Read the ingested data
    df = read_data(ingested_db_path, ingested_table_name)
    # Clean the data
    df = clean_data(df)
    # Write the cleaned data to a new table
    write_data(df, cleaned_db_path, cleaned_table_name)
    
#python3 clean.py --ingested_db_path ../ingest/rep_db.db --ingested_table_name rep_interaction --cleaned_db_path raw_db.db --cleaned_table_name raw_interaction 
if __name__ == "__main__":
    #params 
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--ingested_db_path", type=str,
                        help="path to file from where we should read the data")
    parser.add_argument("--ingested_table_name", type=str,
                        help="table name from where we should read the data")
    parser.add_argument("--cleaned_db_path", type=str,
                        help="path to file where we should save the cleaned data")
    parser.add_argument("--cleaned_table_name", type=str,
                        help="table name where we should save the cleaned data")
    args = parser.parse_args() 
    ingested_db_path = str(args.ingested_db_path)
    ingested_table_name = str(args.ingested_table_name)
    cleaned_db_path = str(args.cleaned_db_path)
    cleaned_table_name = str(args.cleaned_table_name)
    clean(ingested_db_path, ingested_table_name, cleaned_db_path, cleaned_table_name)
    