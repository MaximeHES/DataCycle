import os
import pyodbc
from pathlib import Path
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / "StringConnection_DB.env"

load_dotenv(dotenv_path=ENV_PATH)

def get_connection_values():
    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")

    missing = []
    if not server:
        missing.append("SQL_SERVER")
    if not database:
        missing.append("SQL_DATABASE")
    if not username:
        missing.append("SQL_USERNAME")
    if not password:
        missing.append("SQL_PASSWORD")

    if missing:
        raise ValueError(
            f"Missing environment variable(s) in {ENV_PATH.name}: {', '.join(missing)}"
        )

    return server, database, username, password

def get_connection_string():
    server, database, username, password = get_connection_values()

    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )

def get_pyodbc_conn():
    return pyodbc.connect(get_connection_string())

def get_sqlalchemy_engine():
    conn_str = get_connection_string()
    quoted_conn_str = quote_plus(conn_str)

    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={quoted_conn_str}",
        fast_executemany=True
    )