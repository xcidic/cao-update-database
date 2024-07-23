import psycopg2
import pandas as pd
import yaml
import requests
import urllib.parse
import logging
import re
import http.client
from urllib.parse import urlparse
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

with open('config.yaml', 'r') as file:
    config = yaml.safe_load(file)

PG_HOST = config['database']['host']
PG_PORT = config['database']['port']
PG_DB_NAME = config['database']['db_name']
PG_USERNAME = config['database']['username']
PG_PASSWORD = config['database']['password']

logger = logging.getLogger('UpdateLogger')
logger.setLevel(logging.DEBUG)

# Info handler for successful updates
info_handler = logging.FileHandler('successful_updates_sribu_production_1.log')
info_handler.setLevel(logging.INFO)
info_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
info_handler.setFormatter(info_formatter)

# Error handler for errors and warnings
error_handler = logging.FileHandler('errors_sribu_production_1.log')
error_handler.setLevel(logging.WARNING)
error_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
error_handler.setFormatter(error_formatter)

# Add handlers to the logger
logger.addHandler(info_handler)
logger.addHandler(error_handler)

def replace_url_prefix(url):
    pattern = r'^https://sribulancer-production-sg.*\.com/'
    # pattern = r'https://sribu-dev.3fqlk.upcloudobjects.com/'
    # pattern = r'^https://sribu-2022.*\.com/'
    replacement = 'https://prod-sribu.sniag.upcloudobjects.com/'

    updated_urls = re.sub(pattern, replacement, url) 
    return updated_urls

def is_link_valid(url):
    try:
        parsed_url = urlparse(url)
        connection = http.client.HTTPSConnection(parsed_url.netloc) if parsed_url.scheme == "https" else http.client.HTTPConnection(parsed_url.netloc)
        connection.request("HEAD", parsed_url.path or "/")
        response = connection.getresponse()
       
        return response.status == 200

    except requests.RequestException as e:
        print(f"Request error: {e}")
        return False

def update_links(cursor, table_name, column_name, conn):
    try:
        offset = 0
        pattern = 'https://sribulancer-production-sg'
        success = 0
        while True:
            query = f"SELECT id, {column_name} FROM {table_name} WHERE {column_name} LIKE '{pattern}%' ORDER BY id LIMIT 100000 OFFSET {offset}"
            cursor.execute(query)
            rows = cursor.fetchall()
            if not rows:
                break
            print(len(rows))
            for row in rows:
                id, old_value = row
            
                new_value = old_value
                new_value = replace_url_prefix(new_value)

                if new_value != old_value:
                    if is_link_valid(new_value):
                        try:
                            update_query = f"UPDATE {table_name} SET {column_name} = '{new_value}' WHERE id = '{id}'"
                            cursor.execute(update_query)
                            conn.commit()
                            success += 1
                            logger.info(f"Updated link: {old_value} to {new_value} (ID: {id}, Table: {table_name}, Column: {column_name})")
                        except Exception as error:
                            print(f"Error updating {table_name}.{column_name}: {error}")         
                    else:
                        logger.warning(f"Invalid link: {new_value} {old_value} (ID: {id}, Table: {table_name}, Column: {column_name})")
            offset += 100000
        print(f"Total successful updates: {success}")
            
            

    except Exception as error:
        print(f"Error updating {table_name}.{column_name}: {error}")

def main(csv_file):
    df = pd.read_csv(csv_file)

    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            database=PG_DB_NAME,
            user=PG_USERNAME,
            password=PG_PASSWORD
        )
        cursor = conn.cursor()
  
        for index, row in df.iterrows():
            table_name = row['table']
            column_name = row['column']
     
            print(f"Updating table {table_name}, column {column_name}")
            update_links(cursor, table_name, column_name, conn)

  

        conn.commit()
    except Exception as error:
        print(f"Database error: {error}")
    finally:
        if conn is not None:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    csv_file = 'sribu-2022-1.csv'
    main(csv_file)
