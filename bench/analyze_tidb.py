import pyodbc

conn = pyodbc.connect('DRIVER=MySQL ODBC 8.0 Unicode Driver;SERVER=127.0.0.1;PORT=4000;DATABASE=tpcc;UID=root')
cursor = conn.cursor()
cursor.execute('analyze table CUSTOMER, DISTRICT, HISTORY, ITEM, NATION, NEW_ORDER, ORDERS, ORDER_LINE, REGION, STOCK, SUPPLIER, WAREHOUSE')
conn.close()
