# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011
# Andy Pavlo
# http://www.cs.brown.edu/~pavlo/
#
# Original Java Version:
# Copyright (C) 2008
# Evan Jones
# Massachusetts Institute of Technology
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------

from __future__ import with_statement

import os
import sqlite3
import logging
from pprint import pprint,pformat
from random import randint
from sqlite3.dbapi2 import Cursor
import string
import subprocess
import shutil

import constants
from .abstractdriver import *


## ==============================================
## SqlitepopulateDriver
## ==============================================
class SqlitepopulateDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "database": ("The path to the SQLite database", "/tmp/tpcc.db" ),
    }
    
    def __init__(self, ddl):
        super(SqlitepopulateDriver, self).__init__("sqlitepopulate", ddl)
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return SqlitepopulateDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in SqlitepopulateDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.warehouses = config['warehouses']
        self.scale = config['scalefactor']

        self.database = str(config["database"])
        self.conn = sqlite3.connect("file::memory:?cache=shared")
        self.cursor = self.conn.cursor()

        if config['reset']:
            self.create_schema()


    def create_schema(self):
        self.cursor.execute('''
            CREATE TABLE WAREHOUSE (
            W_ID SMALLINT DEFAULT '0' NOT NULL,
            W_NAME VARCHAR(16) DEFAULT NULL,
            W_STREET_1 VARCHAR(32) DEFAULT NULL,
            W_STREET_2 VARCHAR(32) DEFAULT NULL,
            W_CITY VARCHAR(32) DEFAULT NULL,
            W_STATE VARCHAR(2) DEFAULT NULL,
            W_ZIP VARCHAR(9) DEFAULT NULL,
            W_TAX FLOAT DEFAULT NULL,
            W_YTD FLOAT DEFAULT NULL,
            CONSTRAINT W_PK_ARRAY PRIMARY KEY (W_ID)
            );
        ''')
        self.cursor.execute('''
            CREATE TABLE DISTRICT (
            D_ID TINYINT DEFAULT '0' NOT NULL,
            D_W_ID SMALLINT DEFAULT '0' NOT NULL REFERENCES WAREHOUSE (W_ID),
            D_NAME VARCHAR(16) DEFAULT NULL,
            D_STREET_1 VARCHAR(32) DEFAULT NULL,
            D_STREET_2 VARCHAR(32) DEFAULT NULL,
            D_CITY VARCHAR(32) DEFAULT NULL,
            D_STATE VARCHAR(2) DEFAULT NULL,
            D_ZIP VARCHAR(9) DEFAULT NULL,
            D_TAX FLOAT DEFAULT NULL,
            D_YTD FLOAT DEFAULT NULL,
            D_NEXT_O_ID INT DEFAULT NULL,
            PRIMARY KEY (D_W_ID,D_ID)
            );
        ''')
        self.cursor.execute('''
            CREATE TABLE ITEM (
            I_ID INTEGER DEFAULT '0' NOT NULL,
            I_IM_ID INTEGER DEFAULT NULL,
            I_NAME VARCHAR(32) DEFAULT NULL,
            I_PRICE FLOAT DEFAULT NULL,
            I_DATA VARCHAR(64) DEFAULT NULL,
            CONSTRAINT I_PK_ARRAY PRIMARY KEY (I_ID)
            );
        ''')
        self.cursor.execute('''
            CREATE TABLE CUSTOMER (
            C_ID INTEGER DEFAULT '0' NOT NULL,
            C_D_ID TINYINT DEFAULT '0' NOT NULL,
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
            C_CREDIT_LIM FLOAT DEFAULT NULL,
            C_DISCOUNT FLOAT DEFAULT NULL,
            C_BALANCE FLOAT DEFAULT NULL,
            C_YTD_PAYMENT FLOAT DEFAULT NULL,
            C_PAYMENT_CNT INTEGER DEFAULT NULL,
            C_DELIVERY_CNT INTEGER DEFAULT NULL,
            C_DATA VARCHAR(500),
            PRIMARY KEY (C_W_ID,C_D_ID,C_ID),
            UNIQUE (C_W_ID,C_D_ID,C_LAST,C_FIRST),
            CONSTRAINT C_FKEY_D FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
            );
        ''')
        self.cursor.execute('''CREATE INDEX IDX_CUSTOMER ON CUSTOMER (C_W_ID,C_D_ID,C_LAST);''')
        self.cursor.execute('''
            CREATE TABLE HISTORY (
            H_ID VARCHAR(10) NOT NULL PRIMARY KEY,
            H_C_ID INTEGER DEFAULT NULL,
            H_C_D_ID TINYINT DEFAULT NULL,
            H_C_W_ID SMALLINT DEFAULT NULL,
            H_D_ID TINYINT DEFAULT NULL,
            H_W_ID SMALLINT DEFAULT '0' NOT NULL,
            H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            H_AMOUNT FLOAT DEFAULT NULL,
            H_DATA VARCHAR(32) DEFAULT NULL,
            CONSTRAINT H_FKEY_C FOREIGN KEY (H_C_ID, H_C_D_ID, H_C_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID),
            CONSTRAINT H_FKEY_D FOREIGN KEY (H_D_ID, H_W_ID) REFERENCES DISTRICT (D_ID, D_W_ID)
            );
        ''')
        self.cursor.execute('''
            CREATE TABLE STOCK (
            S_I_ID INTEGER DEFAULT '0' NOT NULL REFERENCES ITEM (I_ID),
            S_W_ID SMALLINT DEFAULT '0 ' NOT NULL REFERENCES WAREHOUSE (W_ID),
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
            PRIMARY KEY (S_W_ID,S_I_ID)
            );
        ''')
        self.cursor.execute('''
            CREATE TABLE ORDERS (
            O_ID INTEGER DEFAULT '0' NOT NULL,
            O_C_ID INTEGER DEFAULT NULL,
            O_D_ID TINYINT DEFAULT '0' NOT NULL,
            O_W_ID SMALLINT DEFAULT '0' NOT NULL,
            O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            O_CARRIER_ID INTEGER DEFAULT NULL,
            O_OL_CNT INTEGER DEFAULT NULL,
            O_ALL_LOCAL INTEGER DEFAULT NULL,
            PRIMARY KEY (O_W_ID,O_D_ID,O_ID),
            UNIQUE (O_W_ID,O_D_ID,O_C_ID,O_ID),
            CONSTRAINT O_FKEY_C FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES CUSTOMER (C_ID, C_D_ID, C_W_ID)
            );
        ''')
        self.cursor.execute('''CREATE INDEX IDX_ORDERS ON ORDERS (O_W_ID,O_D_ID,O_C_ID);''')
        self.cursor.execute('''
            CREATE TABLE NEW_ORDER (
            NO_O_ID INTEGER DEFAULT '0' NOT NULL,
            NO_D_ID TINYINT DEFAULT '0' NOT NULL,
            NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
            CONSTRAINT NO_PK_TREE PRIMARY KEY (NO_D_ID,NO_W_ID,NO_O_ID),
            CONSTRAINT NO_FKEY_O FOREIGN KEY (NO_O_ID, NO_D_ID, NO_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID)
            );
        ''')
        self.cursor.execute('''
            CREATE TABLE ORDER_LINE (
            OL_O_ID INTEGER DEFAULT '0' NOT NULL,
            OL_D_ID TINYINT DEFAULT '0' NOT NULL,
            OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
            OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
            OL_I_ID INTEGER DEFAULT NULL,
            OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
            OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
            OL_QUANTITY INTEGER DEFAULT NULL,
            OL_AMOUNT FLOAT DEFAULT NULL,
            OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
            PRIMARY KEY (OL_W_ID,OL_D_ID,OL_O_ID,OL_NUMBER),
            CONSTRAINT OL_FKEY_O FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES ORDERS (O_ID, O_D_ID, O_W_ID),
            CONSTRAINT OL_FKEY_S FOREIGN KEY (OL_I_ID, OL_SUPPLY_W_ID) REFERENCES STOCK (S_I_ID, S_W_ID)
            );
        ''')
        self.cursor.execute('''CREATE INDEX IDX_ORDER_LINE_TREE ON ORDER_LINE (OL_W_ID,OL_D_ID,OL_O_ID);''')


    ## ----------------------------------------------
    ## loadTuples
    ## ----------------------------------------------
    def loadTuples(self, tableName, tuples):
        if len(tuples) == 0: return
        
        p = ["?"]*len(tuples[0])
        sql = "INSERT INTO %s VALUES (%s)" % (tableName, ",".join(p))
        self.cursor.executemany(sql, tuples)
        
        logging.debug("Loaded %d tuples for tableName %s" % (len(tuples), tableName))
        return

    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        logging.info("Exporting to csv")

        out_folder = f'tpcc_data_{self.warehouses}_{self.scale}'
        #out_folder_mariadb = f'tpcc_data_mariadb_{self.warehouses}_{self.scale}'
        #out_folder_custom = f'tpcc_data_custom_{self.warehouses}_{self.scale}'
        #out_folder_mariadb_custom = f'tpcc_data_mariadb_custom_{self.warehouses}_{self.scale}'

        tables = ['new_order', 'orders', 'order_line', 'customer', 'district', 'stock', 'warehouse', 'history', 'item']
        for table in tables:
            out_file = open(f'{out_folder}/{table}.csv', 'a')
            #out_file_mariadb = open(f'{out_folder_mariadb}/{table}.csv', 'a')
            #out_file_custom = open(f'{out_folder_custom}/{table}.csv', 'a')    
            #out_file_mariadb_custom = open(f'{out_folder_mariadb_custom}/{table}.csv', 'a')    
            self.cursor.execute(f'SELECT * FROM {table}')
        
            for row in self.cursor.fetchall():
                out_file.write('|'.join([str(x) for x in row]) + '\n')
                #out_file_mariadb.write('|'.join([str(x) if x is not None else '\\N' for x in row]) + '\n')
                #out_file_custom.write('|'.join([str(x) for x in row]) + '|false|0|0\n')
                #out_file_mariadb_custom.write('|'.join([str(x) if x is not None else '\\N' for x in row]) + '|0|0|0\n')
        
            out_file.flush()
            out_file.close()
            #out_file_mariadb.flush()
            #out_file_mariadb.close()
            #out_file_custom.flush()
            #out_file_custom.close()
            #out_file_mariadb_custom.flush()
            #out_file_mariadb_custom.close()
