import pyodbc

conn = pyodbc.connect('DRIVER=PostgreSQL Unicode;HOST=127.0.0.1;PORT=5432;DATABASE=tpcc;UID=postgres;PWD=postgres')
cursor = conn.cursor()
cursor.execute('vacuum analyze')
conn.close()
