import os
import pyodbc
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_connection_string():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
    )

def get_pyodbc_conn():
    return pyodbc.connect(get_connection_string())

def get_sqlalchemy_engine():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    conn_str = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
    )
    return create_engine(conn_str, fast_executemany=True)