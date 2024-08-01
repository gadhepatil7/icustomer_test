#with this code we can ingest data from a csv file into a sqlite database

import sqlite3
import pandas as pd
import argparse




def create_database(db_path='sample_data/data.db'):
    """
    Create a SQLite database.
    Parameters
    ----------
    db_path : str
        Path to the database file.
    """
    # Create a connection to the database
    conn = sqlite3.connect(db_path)
    # Close the connection
    conn.close()

def create_table(db_path='sample_data/data.db', table_name='data'):
    """
    Create a table in the SQLite database.
    Parameters
    ----------
    db_path : str
        Path to the database file.
    table_name : str
        Name of the table to create.
    """
    # Create a connection to the database
    conn = sqlite3.connect(db_path)
    # Create a cursor object
    cur = conn.cursor()
    # Create a table
    cur.execute(f'''
        CREATE TABLE {table_name} (
            interaction_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_id INTEGER,
            action TEXT,
            timestamp TEXT
        )
    ''')
    # Commit the transaction
    conn.commit()
    # Close the connection
    conn.close()



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



def ingest_data_into_table(file_path='sample_data/data.csv', db_path='sample_data/data.db', table_name='data'):
    """
    Ingest data from a CSV file into a SQLite database table.
    Parameters
    ----------
    file_path : str
        Path to the CSV file.
    db_path : str
        Path to the SQLite database file.
    """
    # Read the data from the CSV file
    data = pd.read_csv(file_path)
    # define schema and apply it to the data
    data = data.astype({
        'interaction_id': int,
        'user_id': int,
        'product_id': int,
        'action': str,
        'timestamp': str
    })
    # Write the data to the database
    write_data(data, db_path, table_name)

def ingest(file_path, db_path, table_name):
    """
    Ingest data from a CSV file into a SQLite database table.
    Parameters
    ----------
    file_path : str
        Path to the CSV file.
    db_path : str
        Path to the SQLite database file.   
    table_name : str
        Name of the table to write the data to.
    """
    create_database(db_path)
    create_table(db_path, table_name)
    ingest_data_into_table(file_path, db_path, table_name)

#python3 ingest.py --genrated_file_path ../gen_data/data.csv  --db_path rep_db.db  --table_name rep_interaction
if __name__ == "__main__":
    #params 
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--genrated_file_path", type=str,
                        help="path to file from where we should read the data")
    parser.add_argument("--db_path", type=str,
                        help="number of rows we want to generate")
    parser.add_argument("--table_name", type=str,
                        help="path to file where we should save the data")
    args = parser.parse_args() 
    file_path = str(args.genrated_file_path)
    db_path = str(args.db_path)
    table_name = str(args.table_name)
    ingest( file_path, db_path, table_name)
