import pyodbc

conn = pyodbc.connect('DRIVER=/usr/lib/x86_64-linux-gnu/libMonetODBC.so;HOST=127.0.0.1;PORT=50000;DATABASE=tpcc;UID=monetdb;PWD=monetdb')
cursor = conn.cursor()
cursor.execute('analyze sys')
conn.close()
