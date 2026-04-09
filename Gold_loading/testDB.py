# test_connection.py
from gold_utils_v3 import get_connection
import pyodbc

print(pyodbc.drivers())

conn = get_connection()
print("✅ CONNECTION OK")
conn.close()

