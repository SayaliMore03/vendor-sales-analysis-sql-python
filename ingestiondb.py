import pandas as pd
import os
import logging
import time
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(
    filename="OneDrive/Desktop/data/logs/ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# Create a connection to the SQLite database
engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    """
    Ingests a DataFrame into the specified SQLite database table.
    If the table exists, it will be replaced.
    """
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Successfully ingested '{table_name}' into the database.")
    except Exception as e:
        logging.error(f"Error ingesting '{table_name}': {e}")

def load_raw_data():
    """
    Loads all CSV files from the 'data/' directory and ingests them into the database.
    """
    start_time = time.time()
    data_dir = 'data'

    if not os.path.exists(data_dir):
        logging.error(f"Data directory '{data_dir}' does not exist.")
        return

    csv_files = [file for file in os.listdir(data_dir) if file.endswith('.csv')]

    if not csv_files:
        logging.warning(f"No CSV files found in '{data_dir}'.")
        return

    for file in csv_files:
        file_path = os.path.join(data_dir, file)
        try:
            df = pd.read_csv(file_path)
            table_name = os.path.splitext(file)[0]
            ingest_db(df, table_name, engine)
        except Exception as e:
            logging.error(f"Failed to process file '{file}': {e}")

    end_time = time.time()
    total_time = (end_time - start_time) / 60
    logging.info(f"Data ingestion completed in {total_time:.2f} minutes.")

if __name__ == "__main__":
    load_raw_data()
