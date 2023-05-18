from __future__ import with_statement

import os
from random import randint
import traceback
import pyodbc
import logging
from pprint import pformat
import constants
from .abstractdriver import *
import time
import string


TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": "SELECT /*+ read_from_storage(tikv[NEW_ORDER]) */ NO_O_ID FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID > -1 LIMIT 1", #
        "deleteNewOrder": "DELETE FROM NEW_ORDER WHERE NO_D_ID = ? AND NO_W_ID = ? AND NO_O_ID = ?", # d_id, w_id, no_o_id
        "getCId": "SELECT /*+ read_from_storage(tikv[ORDERS]) */ O_C_ID FROM ORDERS WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # no_o_id, d_id, w_id
        "updateOrders": "UPDATE ORDERS SET O_CARRIER_ID = ? WHERE O_ID = ? AND O_D_ID = ? AND O_W_ID = ?", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": "UPDATE ORDER_LINE SET OL_DELIVERY_D = ? WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": "SELECT /*+ read_from_storage(tikv[ORDER_LINE]) */ SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_O_ID = ? AND OL_D_ID = ? AND OL_W_ID = ?", # no_o_id, d_id, w_id
        "updateCustomer": "UPDATE CUSTOMER SET C_BALANCE = C_BALANCE + ? WHERE C_ID = ? AND C_D_ID = ? AND C_W_ID = ?", # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "SELECT W_TAX FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "getDistrict": "SELECT D_TAX, D_NEXT_O_ID FROM DISTRICT WHERE D_ID = ? AND D_W_ID = ?", # d_id, w_id
        "incrementNextOrderId": "UPDATE DISTRICT SET D_NEXT_O_ID = ? WHERE D_ID = ? AND D_W_ID = ?", # d_next_o_id, d_id, w_id
        "getCustomer": "SELECT /*+ read_from_storage(tikv[CUSTOMER]) */ C_DISCOUNT, C_LAST, C_CREDIT FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "createOrder": "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": "INSERT INTO NEW_ORDER (NO_O_ID, NO_D_ID, NO_W_ID) VALUES (?, ?, ?)", # o_id, d_id, w_id
        "getItemInfo": "SELECT /*+ read_from_storage(tikv[ITEM]) */ I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
        "getStockInfo": "SELECT /*+ read_from_storage(tikv[STOCK]) */ S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM STOCK WHERE S_I_ID = ? AND S_W_ID = ?", # d_id, ol_i_id, ol_supply_w_id
        "updateStock": "UPDATE STOCK SET S_QUANTITY = ?, S_YTD = ?, S_ORDER_CNT = ?, S_REMOTE_CNT = ? WHERE S_I_ID = ? AND S_W_ID = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": "INSERT INTO ORDER_LINE (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
    },
    
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "SELECT /*+ read_from_storage(tikv[CUSTOMER]) */ C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT /*+ read_from_storage(tikv[CUSTOMER]) */ C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": "SELECT /*+ read_from_storage(tikv[ORDERS]) */ O_ID, O_CARRIER_ID, O_ENTRY_D FROM ORDERS WHERE O_W_ID = ? AND O_D_ID = ? AND O_C_ID = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": "SELECT /*+ read_from_storage(tikv[ORDER_LINE]) */ OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM ORDER_LINE WHERE OL_W_ID = ? AND OL_D_ID = ? AND OL_O_ID = ?", # w_id, d_id, o_id        
    },
    
    "PAYMENT": {
        "getWarehouse": "SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM WAREHOUSE WHERE W_ID = ?", # w_id
        "updateWarehouseBalance": "UPDATE WAREHOUSE SET W_YTD = W_YTD + ? WHERE W_ID = ?", # h_amount, w_id
        "getDistrict": "SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", # w_id, d_id
        "updateDistrictBalance": "UPDATE DISTRICT SET D_YTD = D_YTD + ? WHERE D_W_ID  = ? AND D_ID = ?", # h_amount, d_w_id, d_id
        "getCustomerByCustomerId": "SELECT /*+ read_from_storage(tikv[CUSTOMER]) */ C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # w_id, d_id, c_id
        "getCustomersByLastName": "SELECT /*+ read_from_storage(tikv[CUSTOMER]) */ C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM CUSTOMER WHERE C_W_ID = ? AND C_D_ID = ? AND C_LAST = ? ORDER BY C_FIRST", # w_id, d_id, c_last
        "updateBCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ?, C_DATA = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": "UPDATE CUSTOMER SET C_BALANCE = ?, C_YTD_PAYMENT = ?, C_PAYMENT_CNT = ? WHERE C_W_ID = ? AND C_D_ID = ? AND C_ID = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": "INSERT INTO HISTORY VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    },
    
    "STOCK_LEVEL": {
        "getOId": "SELECT D_NEXT_O_ID FROM DISTRICT WHERE D_W_ID = ? AND D_ID = ?", 
        "getStockCount": """
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM ORDER_LINE, STOCK
            WHERE OL_W_ID = ?
              AND OL_D_ID = ?
              AND OL_O_ID < ?
              AND OL_O_ID >= ?
              AND S_W_ID = ?
              AND S_I_ID = OL_I_ID
              AND S_QUANTITY < ?
        """,
    },
}

TXN_QUERIES_PREPARED = {
    "DELIVERY": {
        "getNewOrder": "EXECUTE DELIVERY_getNewOrder USING @d_id, @w_id",
        "deleteNewOrder": "EXECUTE DELIVERY_deleteNewOrder USING @d_id, @w_id, @o_id",
        "getCId": "EXECUTE DELIVERY_getCId USING @o_id, @d_id, @w_id",
        "updateOrders": "EXECUTE DELIVERY_updateOrders USING @o_carrier_id, @o_id, @d_id, @w_id",
        "updateOrderLine": "EXECUTE DELIVERY_updateOrderLine USING @ol_delivery_d, @o_id, @d_id, @w_id",
        "sumOLAmount": "EXECUTE DELIVERY_sumOLAmount USING @o_id, @d_id, @w_id",
        "updateCustomer": "EXECUTE DELIVERY_updateCustomer USING @ol_total, @c_id, @d_id, @w_id",
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": "EXECUTE NEW_ORDER_getWarehouseTaxRate USING @w_id",
        "getDistrict": "EXECUTE NEW_ORDER_getDistrict USING @d_id, @w_id",
        "incrementNextOrderId": "EXECUTE NEW_ORDER_incrementNextOrderId USING @d_next_o_id, @d_id, @w_id",
        "getCustomer": "EXECUTE NEW_ORDER_getCustomer USING @w_id, @d_id, @c_id",
        "createOrder": "EXECUTE NEW_ORDER_createOrder USING @o_id, @d_id, @w_id, @c_id, @o_entry_d, @o_carrier_id, @o_ol_cnt, @o_all_local",
        "createNewOrder": "EXECUTE NEW_ORDER_createNewOrder USING @o_id, @d_id, @w_id",
        "getItemInfo": "EXECUTE NEW_ORDER_getItemInfo USING @i_id",
        "getStockInfo": lambda x: "EXECUTE NEW_ORDER_getStockInfo_%02d USING @i_id, @ol_supply_w_id" % x,
        "updateStock": "EXECUTE NEW_ORDER_updateStock USING @s_quantity, @s_ytd, @s_order_cnt, @s_remote_cnt, @i_id, @ol_supply_w_id",
        "createOrderLine": "EXECUTE NEW_ORDER_createOrderLine USING @o_id, @d_id, @w_id, @ol_number, @i_id, @ol_supply_w_id, @ol_delivery_d, @ol_quantity, @ol_amount, @ol_dist_info",
    },
    "ORDER_STATUS": {
        "getCustomerByCustomerId": "EXECUTE ORDER_STATUS_getCustomerByCustomerId USING @w_id, @d_id, @c_id",
        "getCustomersByLastName": "EXECUTE ORDER_STATUS_getCustomersByLastName USING @w_id, @d_id, @c_last",
        "getLastOrder": "EXECUTE ORDER_STATUS_getLastOrder USING @w_id, @d_id, @c_id",
        "getOrderLines": "EXECUTE ORDER_STATUS_getOrderLines USING @w_id, @d_id, @o_id",
    },
    "PAYMENT": {
        "getWarehouse": "EXECUTE PAYMENT_getWarehouse USING @w_id",
        "updateWarehouseBalance": "EXECUTE PAYMENT_updateWarehouseBalance USING @h_amount, @w_id",
        "getDistrict": "EXECUTE PAYMENT_getDistrict USING @w_id, @d_id",
        "updateDistrictBalance": "EXECUTE PAYMENT_updateDistrictBalance USING @h_amount, @w_id, @d_id",
        "getCustomerByCustomerId": "EXECUTE PAYMENT_getCustomerByCustomerId USING @w_id, @c_d_id, @c_id",
        "getCustomersByLastName": "EXECUTE PAYMENT_getCustomersByLastName USING @w_id, @c_d_id, @c_last",
        "updateBCCustomer": "EXECUTE PAYMENT_updateBCCustomer USING @c_balance, @h_amount, @c_payment_cnt, @c_data, @w_id, @d_id, @c_id",
        "updateGCCustomer": "EXECUTE PAYMENT_updateGCCustomer USING @c_balance, @h_amount, @c_payment_cnt, @w_id, @d_id, @c_id",
        "insertHistory": "EXECUTE PAYMENT_insertHistory USING @h_id, @c_id, @c_d_id, @c_w_id, @d_id, @w_id, @h_date, @h_amount, @h_data",
    },
    "STOCK_LEVEL": {
        "getOId": "EXECUTE STOCK_LEVEL_getOId USING @w_id, @d_id", 
        "getStockCount": "EXECUTE STOCK_LEVEL_getStockCount USING @w_id, @d_id, @o_id_upper, @o_id_lower, @w_id, @s_quantity"
    }
}

SCHEMA = '''
    CREATE TABLE WAREHOUSE (
        W_ID SMALLINT DEFAULT '0' NOT NULL,
        W_NAME VARCHAR(16) DEFAULT NULL,
        W_STREET_1 VARCHAR(32) DEFAULT NULL,
        W_STREET_2 VARCHAR(32) DEFAULT NULL,
        W_CITY VARCHAR(32) DEFAULT NULL,
        W_STATE VARCHAR(2) DEFAULT NULL,
        W_ZIP VARCHAR(9) DEFAULT NULL,
        W_TAX decimal(18,4) DEFAULT NULL,
        W_YTD decimal(18,4) DEFAULT NULL,
        primary key (W_ID)
    );

    CREATE TABLE DISTRICT (
        D_ID SMALLINT DEFAULT '0' NOT NULL,
        D_W_ID SMALLINT DEFAULT '0' NOT NULL,
        D_NAME VARCHAR(16) DEFAULT NULL,
        D_STREET_1 VARCHAR(32) DEFAULT NULL,
        D_STREET_2 VARCHAR(32) DEFAULT NULL,
        D_CITY VARCHAR(32) DEFAULT NULL,
        D_STATE VARCHAR(2) DEFAULT NULL,
        D_ZIP VARCHAR(9) DEFAULT NULL,
        D_TAX decimal(18,4) DEFAULT NULL,
        D_YTD decimal(18,4) DEFAULT NULL,
        D_NEXT_O_ID INT DEFAULT NULL,
        primary key (D_W_ID, D_ID)
    );

    CREATE TABLE ITEM (
        I_ID INTEGER DEFAULT '0' NOT NULL,
        I_IM_ID INTEGER DEFAULT NULL,
        I_NAME VARCHAR(32) DEFAULT NULL,
        I_PRICE decimal(18,4) DEFAULT NULL,
        I_DATA VARCHAR(64) DEFAULT NULL,
        primary key (I_ID)
    );
    ALTER TABLE ITEM SET TIFLASH REPLICA 1;

    CREATE TABLE CUSTOMER (
        C_ID INTEGER DEFAULT '0' NOT NULL,
        C_D_ID SMALLINT DEFAULT '0' NOT NULL,
        C_W_ID SMALLINT DEFAULT '0' NOT NULL,
        C_FIRST VARCHAR(32) DEFAULT NULL,
        C_MIDDLE VARCHAR(2) DEFAULT NULL,
        C_LAST VARCHAR(32) DEFAULT NULL,
        C_STREET_1 VARCHAR(32) DEFAULT NULL,
        C_STREET_2 VARCHAR(32) DEFAULT NULL,
        C_CITY VARCHAR(32) DEFAULT NULL,
        C_STATE VARCHAR(2) DEFAULT NULL,
        C_ZIP VARCHAR(9) DEFAULT NULL,
        C_PHONE VARCHAR(32) DEFAULT NULL,
        C_SINCE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        C_CREDIT VARCHAR(2) DEFAULT NULL,
        C_CREDIT_LIM decimal(18,4) DEFAULT NULL,
        C_DISCOUNT decimal(18,4) DEFAULT NULL,
        C_BALANCE decimal(18,4) DEFAULT NULL,
        C_YTD_PAYMENT decimal(18,4) DEFAULT NULL,
        C_PAYMENT_CNT INTEGER DEFAULT NULL,
        C_DELIVERY_CNT INTEGER DEFAULT NULL,
        C_DATA VARCHAR(500),
        primary key (c_w_id, c_d_id, c_id)
    );
    CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);
    ALTER TABLE CUSTOMER SET TIFLASH REPLICA 1;

    CREATE TABLE HISTORY (
        H_ID VARCHAR(10) NOT NULL,
        H_C_ID INTEGER DEFAULT NULL,
        H_C_D_ID SMALLINT DEFAULT NULL,
        H_C_W_ID SMALLINT DEFAULT NULL,
        H_D_ID SMALLINT DEFAULT NULL,
        H_W_ID SMALLINT DEFAULT '0' NOT NULL,
        H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        H_AMOUNT decimal(18,4) DEFAULT NULL,
        H_DATA VARCHAR(32) DEFAULT NULL,
        primary key (h_id)
    );

    CREATE TABLE STOCK (
        S_I_ID INTEGER DEFAULT '0' NOT NULL,
        S_W_ID SMALLINT DEFAULT '0 ' NOT NULL,
        S_QUANTITY INTEGER DEFAULT '0' NOT NULL,
        S_DIST_01 VARCHAR(32) DEFAULT NULL,
        S_DIST_02 VARCHAR(32) DEFAULT NULL,
        S_DIST_03 VARCHAR(32) DEFAULT NULL,
        S_DIST_04 VARCHAR(32) DEFAULT NULL,
        S_DIST_05 VARCHAR(32) DEFAULT NULL,
        S_DIST_06 VARCHAR(32) DEFAULT NULL,
        S_DIST_07 VARCHAR(32) DEFAULT NULL,
        S_DIST_08 VARCHAR(32) DEFAULT NULL,
        S_DIST_09 VARCHAR(32) DEFAULT NULL,
        S_DIST_10 VARCHAR(32) DEFAULT NULL,
        S_YTD INTEGER DEFAULT NULL,
        S_ORDER_CNT INTEGER DEFAULT NULL,
        S_REMOTE_CNT INTEGER DEFAULT NULL,
        S_DATA VARCHAR(64) DEFAULT NULL,
        primary key (S_W_ID,S_I_ID)
    );
    ALTER TABLE STOCK SET TIFLASH REPLICA 1;

    CREATE TABLE ORDERS (
        O_ID INTEGER DEFAULT '0' NOT NULL,
        O_C_ID INTEGER DEFAULT NULL,
        O_D_ID SMALLINT DEFAULT '0' NOT NULL,
        O_W_ID SMALLINT DEFAULT '0' NOT NULL,
        O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        O_CARRIER_ID INTEGER DEFAULT NULL,
        O_OL_CNT INTEGER DEFAULT NULL,
        O_ALL_LOCAL INTEGER DEFAULT NULL,
        primary key (O_W_ID,O_D_ID,O_ID)
    );
    CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);
    ALTER TABLE ORDERS SET TIFLASH REPLICA 1;

    CREATE TABLE NEW_ORDER (
        NO_O_ID INTEGER DEFAULT '0' NOT NULL,
        NO_D_ID SMALLINT DEFAULT '0' NOT NULL,
        NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
        primary key (NO_W_ID,NO_D_ID,NO_O_ID)
    );
    ALTER TABLE NEW_ORDER SET TIFLASH REPLICA 1;

    CREATE TABLE ORDER_LINE (
        OL_O_ID INTEGER DEFAULT '0' NOT NULL,
        OL_D_ID SMALLINT DEFAULT '0' NOT NULL,
        OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
        OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
        OL_I_ID INTEGER DEFAULT NULL,
        OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
        OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
        OL_QUANTITY INTEGER DEFAULT NULL,
        OL_AMOUNT decimal(18,4) DEFAULT NULL,
        OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
        primary key (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER)
    );
    ALTER TABLE ORDER_LINE SET TIFLASH REPLICA 1;


    CREATE TABLE NATION (
        n_nationkey integer NOT NULL PRIMARY KEY,
        n_name character(25) NOT NULL,
        n_regionkey integer NOT NULL,
        n_comment character(152) NOT NULL
    );
    ALTER TABLE NATION SET TIFLASH REPLICA 1;

    CREATE TABLE REGION (
        r_regionkey integer NOT NULL PRIMARY KEY,
        r_name character(55) NOT NULL,
        r_comment character(152) NOT NULL
    );
    ALTER TABLE REGION SET TIFLASH REPLICA 1;

    CREATE TABLE SUPPLIER (
        su_suppkey integer NOT NULL PRIMARY KEY,
        su_name character(25) NOT NULL,
        su_address character varying(40) NOT NULL,
        su_nationkey integer NOT NULL,
        su_phone character(15) NOT NULL,
        su_acctbal numeric(12,2) NOT NULL,
        su_comment character(101) NOT NULL
    );
    ALTER TABLE SUPPLIER SET TIFLASH REPLICA 1;
'''


## ==============================================
## TidbDriver
## ==============================================
class TidbDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "driver": ("The odbc driver used", "MySQL ODBC 8.0 Unicode Driver" ),
        "host": ("The server address", "127.0.0.1"),
        "port": ("The server port", "4000"),
        "database": ("The database used", "tpcc"),
        "username": ("The username", "root"),
        "password": ("The password", "")
    }
    

    def __init__(self, ddl):
        super(TidbDriver, self).__init__("singlestore", ddl)

    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return TidbDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in TidbDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = str(config["database"])
        self.warehouses = config['warehouses']
        self.scale = config['scalefactor']
        self.conn = pyodbc.connect(
            'DRIVER={' + config['driver'] + 
            '};SERVER=' + config['host'] + 
            ';PORT=' + config['port'] + 
            ';DATABASE=' + config['database'] + 
            ';UID=' + config['username'] + 
            ';PWD=' + config['password'] +
            ';ENABLE_LOCAL_INFILE=1'
        )
        self.cursor = self.conn.cursor()
        self.cursor.execute("SET GLOBAL tidb_multi_statement_mode='ON'")
        self.cursor.execute("SET SESSION tidb_multi_statement_mode='ON'")
        #self.cursor.execute("SET SESSION tidb_txn_mode = 'pessimistic'")
        # optimistic has better performance than pessimistic, which is the default
        self.cursor.execute("SET SESSION tidb_txn_mode = 'optimistic'")
        self.cursor.execute("SET GLOBAL tidb_server_memory_limit = 0")
        self.conn.autocommit = False

        if config['reset']:
            self.createDb()

        if config['load_from_csv']:
            self.loadFromCsv()
        
        self.prepareStatements()


    def createDb(self):
        print('Creating database')
        tables = ['NEW_ORDER', 'ORDERS', 'ORDER_LINE', 'CUSTOMER', 'DISTRICT', 'STOCK', 'WAREHOUSE', 
                  'HISTORY', 'ITEM', 'LOG', 'NATION', 'REGION', 'SUPPLIER']

        for table in tables:
            try:
                self.cursor.execute('DROP TABLE ' + table)
                self.cursor.execute('commit')
            except:
                self.cursor.execute('rollback')

        self.cursor.execute(SCHEMA)
        self.cursor.execute('commit')


    def importCsv(self, tablename, filename):
        self.cursor.execute(f'''
            LOAD DATA LOCAL INFILE '{filename}' 
            INTO TABLE {tablename}
            FIELDS TERMINATED BY '|' ENCLOSED BY '"' LINES TERMINATED BY '\n'
        ''')


    def loadFromCsv(self):
        self.cursor.execute('set global tidb_enable_batch_dml = 1')
        self.cursor.execute('set session tidb_batch_insert = 1')
        self.cursor.execute('set session tidb_dml_batch_size = 50000')
        print('Loading from csv')
        tables = ['ITEM', 'WAREHOUSE', 'NEW_ORDER', 'ORDERS', 'ORDER_LINE', 'CUSTOMER', 'DISTRICT', 
                  'STOCK', 'HISTORY']
        data_folder = f'tpcc_data_{self.warehouses}_{self.scale}'

        for table in tables:
            filename = os.path.join(os.path.abspath(os.getcwd()), data_folder, 
                                    table.lower() + (("_mysql" if table == "ORDER_LINE" else "")) + '.csv')
            try:
                self.importCsv(table, filename)
            except Exception as e:
                print(e)
                exit(f'Csv file not found: {filename}')
        
        tables_ch = ['NATION', 'REGION', 'SUPPLIER']
        for table in tables_ch:
            filename = os.path.join(os.path.abspath(os.getcwd()), 'chbench_data', table.lower() + '.csv')
            try:
                self.importCsv(table, filename)
            except Exception as e:
                print(e)
                print(f'Warning: ch-bench csv file not found: {filename}')
        self.cursor.execute('commit')
        
    
    def prepareStatements(self):
        for function, statements in TXN_QUERIES.items():
            for statementName, sql in statements.items():
                if statementName == 'getStockInfo':
                    for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                        ps = f'''PREPARE {function}_{statementName}_{'%02d' % d_id} FROM "{sql % d_id}"'''
                        self.cursor.execute(ps)
                else:
                    ps = f'PREPARE {function}_{statementName} FROM "{sql}"'
                    self.cursor.execute(ps)


    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        p = ["?"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (tableName, ",".join(p))
        print('Populating ' + tableName)

        self.cursor.executemany(sql, tuples)
        self.cursor.execute('commit')

        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return


    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        self.conn.autocommit = False


    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        try:
            q = TXN_QUERIES_PREPARED["DELIVERY"]
            
            w_id = params["w_id"]
            o_carrier_id = params["o_carrier_id"]
            ol_delivery_d = params["ol_delivery_d"]
            
            self.cursor.execute(f"set @w_id = {w_id}; set @o_carrier_id = {o_carrier_id}; set @ol_delivery_d = '{ol_delivery_d}'")

            result = [ ]
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                self.cursor.execute(f"set @d_id = {d_id}")
                self.cursor.execute(q["getNewOrder"])
                newOrder = self.cursor.fetchone()
                if newOrder == None:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                assert len(newOrder) > 0
                no_o_id = newOrder[0]
                
                self.cursor.execute(f"set @o_id = {no_o_id}")
                self.cursor.execute(q["getCId"])
                c_id = self.cursor.fetchone()[0]
                
                self.cursor.execute(q["sumOLAmount"])
                ol_total = self.cursor.fetchone()[0]

                self.cursor.execute(q["deleteNewOrder"])
                self.cursor.execute(q["updateOrders"])
                self.cursor.execute(q["updateOrderLine"])

                # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                # them out
                # If there are no order lines, SUM returns null. There should always be order lines.
                assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
                assert ol_total > 0.0
                
                self.cursor.execute(f"set @ol_total = {ol_total}; set @c_id = {c_id}")
                self.cursor.execute(q["updateCustomer"])

                result.append((d_id, no_o_id))
            ## FOR

            self.cursor.execute('commit')
            return result
        except Exception as e:
            self.cursor.execute('rollback')
            raise e


    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        try:
            q = TXN_QUERIES_PREPARED["NEW_ORDER"]
            
            w_id = params["w_id"]
            d_id = params["d_id"]
            c_id = params["c_id"]
            o_entry_d = params["o_entry_d"]
            i_ids = params["i_ids"]
            i_w_ids = params["i_w_ids"]
            i_qtys = params["i_qtys"]
                
            assert len(i_ids) > 0
            assert len(i_ids) == len(i_w_ids)
            assert len(i_ids) == len(i_qtys)

            all_local = True
            items = [ ]
            for i in range(len(i_ids)):
                ## Determine if this is an all local order or not
                all_local = all_local and i_w_ids[i] == w_id
                self.cursor.execute(f"set @i_id = {i_ids[i]}")
                self.cursor.execute(q["getItemInfo"])
                items.append(self.cursor.fetchone())
            assert len(items) == len(i_ids)
            
            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            for item in items:
                if len(item) == 0:
                    self.cursor.execute('rollback')
                    return
            ## FOR
            
            self.cursor.execute(f"set @w_id = {w_id}; set @d_id = {d_id}; set @c_id = {c_id}; set @o_entry_d = '{o_entry_d}'; set @o_all_local = {all_local}")
            
            ## ----------------
            ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
            ## ----------------
            self.cursor.execute(q["getWarehouseTaxRate"])
            w_tax = self.cursor.fetchone()[0]
            
            self.cursor.execute(q["getDistrict"])
            district_info = self.cursor.fetchone()
            d_tax = district_info[0]
            d_next_o_id = district_info[1]
            
            self.cursor.execute(q["getCustomer"])
            customer_info = self.cursor.fetchone()
            c_discount = customer_info[0]

            ## ----------------
            ## Insert Order Information
            ## ----------------
            ol_cnt = len(i_ids)
            o_carrier_id = constants.NULL_CARRIER_ID
            
            self.cursor.execute(f"set @o_id = {d_next_o_id}; set @d_next_o_id = {d_next_o_id + 1}; set @o_carrier_id = {o_carrier_id}; set @o_ol_cnt = {ol_cnt}")
            
            self.cursor.execute(q["incrementNextOrderId"])
            self.cursor.execute(q["createOrder"])
            self.cursor.execute(q["createNewOrder"])

            ## ----------------
            ## Insert Order Item Information
            ## ----------------
            item_data = [ ]
            total = 0
            for i in range(len(i_ids)):
                ol_number = i + 1
                ol_supply_w_id = i_w_ids[i]
                ol_i_id = i_ids[i]
                ol_quantity = i_qtys[i]

                itemInfo = items[i]
                i_name = itemInfo[1]
                i_data = itemInfo[2]
                i_price = itemInfo[0]

                self.cursor.execute(f"set @i_id = {ol_i_id}; set @ol_supply_w_id = {ol_supply_w_id}")
                self.cursor.execute(q["getStockInfo"](d_id))
                stockInfo = self.cursor.fetchone()
                if len(stockInfo) == 0:
                    logging.warn("No STOCK record for (ol_i_id=%d, ol_supply_w_id=%d)" % (ol_i_id, ol_supply_w_id))
                    continue
                s_quantity = stockInfo[0]
                s_ytd = stockInfo[2]
                s_order_cnt = stockInfo[3]
                s_remote_cnt = stockInfo[4]
                s_data = stockInfo[1]
                s_dist_xx = stockInfo[5] # Fetches data from the s_dist_[d_id] column

                ## Update stock
                s_ytd += ol_quantity
                if s_quantity >= ol_quantity + 10:
                    s_quantity = s_quantity - ol_quantity
                else:
                    s_quantity = s_quantity + 91 - ol_quantity
                s_order_cnt += 1
                
                if ol_supply_w_id != w_id: s_remote_cnt += 1
                
                ## Transaction profile states to use "ol_quantity * i_price"
                ol_amount = ol_quantity * i_price
                total += ol_amount

                self.cursor.execute(f"set @s_quantity = {s_quantity}; set @s_ytd = {s_ytd}; set @s_order_cnt = {s_order_cnt}; set @s_remote_cnt = {s_remote_cnt}; set @ol_number = {ol_number}; set @ol_amount = {ol_amount}; set @ol_dist_info = '{s_dist_xx}'")
                self.cursor.execute(q["updateStock"])

                if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                    brand_generic = 'B'
                else:
                    brand_generic = 'G'

                self.cursor.execute(q["createOrderLine"])

                ## Add the info to be returned
                item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
            ## FOR
            
            ## Commit!
            self.cursor.execute('commit')

            ## Adjust the total for the discount
            #\ "c_discount:", c_discount, type(c_discount)
            #print "w_tax:", w_tax, type(w_tax)
            #print "d_tax:", d_tax, type(d_tax)
            total *= (1 - c_discount) * (1 + w_tax + d_tax)

            ## Pack up values the client is missing (see TPC-C 2.4.3.5)
            misc = [ (w_tax, d_tax, d_next_o_id, total) ]
            
            return [ customer_info, misc, item_data ]
        except Exception as e:
            self.cursor.execute('rollback')
            raise e


    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        q = TXN_QUERIES_PREPARED["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)
        
        self.cursor.execute(f"set @w_id = {w_id}; set @d_id = {d_id}; set @c_id = {c_id}; set @c_last = '{c_last}';")

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"])
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"])
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = int((namecnt-1)/2)
            customer = all_customers[index]
            c_id = customer[0]
            self.cursor.execute(f"set @c_id = {c_id}")
        assert len(customer) > 0
        assert c_id != None

        self.cursor.execute(q["getLastOrder"])
        order = self.cursor.fetchone()
        if order:
            self.cursor.execute(f"set @o_id = {order[0]}")
            self.cursor.execute(q["getOrderLines"])
            orderLines = self.cursor.fetchall()
        else:
            orderLines = [ ]

        return [ customer, order, orderLines ]


    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------  
    def doPayment(self, params):
        try:
            #begin_exec = time.time()
            q = TXN_QUERIES_PREPARED["PAYMENT"]

            w_id = params["w_id"]
            d_id = params["d_id"]
            h_amount = params["h_amount"]
            c_w_id = params["c_w_id"]
            c_d_id = params["c_d_id"]
            c_id = params["c_id"]
            c_last = params["c_last"]
            h_date = params["h_date"]
            
            self.cursor.execute(f"set @w_id = {w_id}; set @d_id = {d_id}; set @c_id = {c_id}; set @c_last = '{c_last}'; set @c_d_id = {c_d_id}; set @h_amount = {h_amount}; set @h_date = '{h_date}'")

            if c_id != None:
                #begin_read_customer = time.time()
                self.cursor.execute(q["getCustomerByCustomerId"])
                customer = self.cursor.fetchone()
            else:
                # Get the midpoint customer's id
                #begin_read_customer = time.time()
                self.cursor.execute(q["getCustomersByLastName"])
                all_customers = self.cursor.fetchall()
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = int((namecnt-1)/2)
                customer = all_customers[index]
                c_id = customer[0]
            assert len(customer) > 0

            c_balance = float(customer[14]) - h_amount
            #c_ytd_payment = customer[15] + h_amount
            c_payment_cnt = customer[16] + 1
            c_data = customer[17]

            #begin_read_warehouse = time.time()
            self.cursor.execute(q["getWarehouse"])
            warehouse = self.cursor.fetchone()
            
            #begin_read_district = time.time()
            self.cursor.execute(q["getDistrict"])
            district = self.cursor.fetchone()
            
            self.cursor.execute(q["updateWarehouseBalance"])
            self.cursor.execute(q["updateDistrictBalance"])

            # Concatenate w_name, four spaces, d_name
            h_data = "%s    %s" % (warehouse[0], district[0])
            h_id = ''.join([string.ascii_letters[randint(0, len(string.ascii_letters) - 1)] for _ in range(6)])

            # Customer Credit Information
            if customer[11] == constants.BAD_CREDIT:
                newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
                c_data = (newData + "|" + c_data)
                if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
                self.cursor.execute(f"set @c_id = {c_id}; set @c_data  = '{c_data}'; set @c_balance = {c_balance}; set @c_payment_cnt = {c_payment_cnt}; set @c_last = '{c_last}'; set @h_amount = {h_amount}; set @h_id = {h_id}; set h_data = '{h_data}';")
                self.cursor.execute(q["updateBCCustomer"])
            else:
                c_data = ""
                self.cursor.execute(f"set @c_id = {c_id}; set @c_data = '{c_data}'; set @c_balance = {c_balance}; set @c_payment_cnt = {c_payment_cnt}; set @c_last = '{c_last}'; set @h_amount = {h_amount}; set @h_id = {h_id}; set h_data = '{h_data}';")
                self.cursor.execute(q["updateGCCustomer"])

            # Create the history record
            self.cursor.execute(q["insertHistory"])

            self.cursor.execute('commit')

            # TPC-C 2.5.3.3: Must display the following fields:
            # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
            # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
            # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
            # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
            # H_AMOUNT, and H_DATE.

            # Hand back all the warehouse, district, and customer data
            return [ warehouse, district, customer ]
        
        except Exception as e:
            self.cursor.execute('rollback')
            raise e


    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        q = TXN_QUERIES_PREPARED["STOCK_LEVEL"]

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        self.cursor.execute(f"set @w_id = {w_id}; set @d_id = {d_id}; set @s_quantity = {threshold}")
        self.cursor.execute(q["getOId"])
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        self.cursor.execute(f"set @o_id_upper = {o_id}; set @o_id_lower = {o_id - 20};")
        self.cursor.execute(q["getStockCount"])
        result = self.cursor.fetchone()
        
        return int(result[0])
        
## CLASS