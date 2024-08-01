# Description: This file contains a function that generates random data for the user-product interaction dataset.
import pandas as pd
import random
from datetime import datetime, timedelta
from collections import Counter
import argparse

def genrate_data_store_to_path(num_rows=1000, file_path='data.csv'):
    """
    Generate random data for the user-product interaction dataset and store it in a CSV file.
    Parameters
    ----------
    num_rows : int
        Number of rows to generate.
    file_path : str
        Path to the file where the data should be stored.
    """
    # genrate random data
    interaction_ids = list(range(1, num_rows + 1))
    user_ids = [random.randint(1, 100) for _ in range(num_rows)]  # 100 unique users
    product_ids = [random.randint(1, 50) for _ in range(num_rows)]  # 50 unique products
    actions = [random.choice(['view', 'click', 'purchase']) for _ in range(num_rows)]
    timestamps = [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(num_rows)]
    # Create a DataFrame
    data = {
        'interaction_id': interaction_ids,
        'user_id': user_ids,
        'product_id': product_ids,
        'action': actions,
        'timestamp': timestamps
    }
    df = pd.DataFrame(data)
    # Store the data in a CSV file
    df.to_csv(file_path, index=False)
    

#python3 genrate_data.py --num_rows 1000 --file_path data.csv
if __name__ == "__main__":
    #params 
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--num_rows", type=str,
                        help="number of rows we want to generate")
    parser.add_argument("--file_path", type=str,
                        help="path to file where we should save the data")
    args = parser.parse_args() 
    num_rows = int(args.num_rows)
    file_path = str(args.file_path)
    # genrate data
    genrate_data_store_to_path(num_rows, file_path)


