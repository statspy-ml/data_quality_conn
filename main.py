import psycopg2
import oracledb
import smtplib
from email.mime.text import MIMEText


import platform
import oracledb
import os



d = os.environ.get("HOME")+("/Downloads/instantclient_19_16")

oracledb.init_oracle_client(lib_dir=d)

def connect_redshift():
    
    try:
        conn = psycopg2.connect(
            dbname='poc-rd',
            user='',
            password='',
            host='',
            port='5440'
        )
        return conn
    except Exception as e:
        print("Error connecting to Redshift: ", e)
        return None

def connect_oracle():
   
    try:
        conn = oracledb.connect(
            user='',
            password='s',
            dsn=''
        )
        return conn
    except Exception as e:
        print("Error connecting to Oracle: ", e)
        return None
    

def fetch_row_count_redshift(connection, table_name):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = cursor.fetchone()
            return result[0]  # Return the count
    except Exception as e:
        print("Error querying Redshift: ", e)
        return None

def fetch_row_count_oracle(connection, table_name):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            result = cursor.fetchone()
            return result[0]  # Return the count
    except Exception as e:
        print("Error querying Oracle: ", e)
        return None

def main():
    redshift_conn = connect_redshift()
    oracle_conn = connect_oracle()

    redshift_count = fetch_row_count_redshift(redshift_conn, 'rd_ext_vuc_decrypt.vuc')
    oracle_count = fetch_row_count_oracle(oracle_conn, 'schema_tabela_oracle')

    print(f"Volumetria Redshift: {redshift_count}")
    print(f"Volumetria Oracle: {oracle_count}")

    if redshift_count == oracle_count:
        print("data_quality check passed")
    else:
        print("data_quality check failed")

    if redshift_conn:
        redshift_conn.close()
    if oracle_conn:
        oracle_conn.close()

if __name__ == "__main__":
    main()
