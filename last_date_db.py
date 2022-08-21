import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine
import pymysql
from datetime import datetime as dt
import time
pymysql.install_as_MySQLdb()
from dotenv import load_dotenv
import os

load_dotenv() #call .env

def latest_date_db():
    database_username = os.getenv("USERNAME")+"am"
    database_password = os.getenv("PASSWORD")
    database_ip       = os.getenv("IP_ADDRESS")
    database_name     = os.getenv("DB_NAME")
    port = os.getenv("PORT")
    # database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
    #                                                format(database_username, database_password, 
    #                                                       database_ip, database_name))

    database_connection = 'mysql+mysqldb://{0}:{1}@{2}:{3}/{4}?charset=utf8mb4'.format(database_username, 
                                                                                    database_password,
                                                                                    database_ip, port, 
                                                                                    database_name)
    print(database_connection)
    engine = create_engine(database_connection, echo=False)

    #connect to the database
    connection = pymysql.connect(host=database_ip, user=database_username, password=database_password, db=database_name)
    #create cursor
    cursor = connection.cursor()

    df = pd.read_sql('SELECT max(date(`Tanggal Daftar`)) as max_date_db FROM pse_kominfo_data', con=connection)
    max_date_db = df['max_date_db'][0]
    return max_date_db