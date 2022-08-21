from ast import main
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
from main import main_data

load_dotenv() #call .env


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