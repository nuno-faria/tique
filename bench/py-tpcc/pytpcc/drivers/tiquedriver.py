from __future__ import with_statement

import os
from random import randint
import string
import pyodbc 
import logging
from pprint import pformat
import constants
from .abstractdriver import *
import time
from collections import defaultdict
import hashlib


COLUMNS = {
    'WAREHOUSE': {
        #'keys': ['W_ID'],
        'keys': ['W_ID'],
        'values': ['W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD'],
        'all': ['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD']
    },
    'DISTRICT': {
        #'keys': ['D_W_ID', 'D_ID'],
        'keys': ['pk'],
        'values': ['D_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID'],
        'all': ['D_W_ID', 'D_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID', 'pk']
    },
    'ITEM': {
        #'keys': ['I_ID'],
        'keys': ['I_ID'],
        'values': ['I_IM_ID', 'I_NAME', 'I_PRICE', 'I_DATA'],
        'all': ['I_ID', 'I_IM_ID', 'I_NAME', 'I_PRICE', 'I_DATA']
    },
    'CUSTOMER': {
        #'keys': ['C_W_ID','C_D_ID','C_ID'],
        'keys': ['pk'],
        'values': ['C_FIRST','C_MIDDLE','C_LAST','C_STREET_1','C_STREET_2','C_CITY','C_STATE','C_ZIP','C_PHONE','C_SINCE','C_CREDIT','C_CREDIT_LIM','C_DISCOUNT','C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT','C_DATA'],
        'all': ['C_W_ID','C_D_ID','C_ID','C_FIRST','C_MIDDLE','C_LAST','C_STREET_1','C_STREET_2','C_CITY','C_STATE','C_ZIP','C_PHONE','C_SINCE','C_CREDIT','C_CREDIT_LIM','C_DISCOUNT','C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT','C_DATA', 'pk', 'pk_last']
    },
    'STOCK': {
        #'keys': ['S_W_ID', 'S_I_ID'],
        'keys': ['pk'],
        'values': ['S_QUANTITY', 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DATA'],
        'all': ['S_W_ID', 'S_I_ID', 'S_QUANTITY', 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DATA', 'pk']
    },
    'ORDERS': {
        #'keys': ['O_W_ID','O_D_ID','O_ID'],
        'keys': ['pk'],
        'values': ['O_C_ID','O_ENTRY_D','O_CARRIER_ID','O_OL_CNT','O_ALL_LOCAL'],
        'all': ['O_W_ID','O_D_ID','O_ID','O_C_ID','O_ENTRY_D','O_CARRIER_ID','O_OL_CNT','O_ALL_LOCAL', 'pk']
    },
    'NEW_ORDER': {
        #'keys': ['NO_W_ID','NO_D_ID','NO_O_ID'],
        'keys': ['pk'],
        'values': [],
        'all': ['NO_W_ID','NO_D_ID','NO_O_ID', 'pk', 'pk_partial']
    },
    'ORDER_LINE': {
        #'keys': ['OL_W_ID', 'OL_D_ID', 'OL_O_ID'],
        'keys': ['pk'],
        'values': ['OL_NUMBER', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_DELIVERY_D', 'OL_QUANTITY', 'OL_AMOUNT', 'OL_DIST_INFO'],
        'all': ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_DELIVERY_D', 'OL_QUANTITY', 'OL_AMOUNT', 'OL_DIST_INFO', 'pk', 'pk_partial']
    },
}


def keys_to_string(table):
    return ', '.join(COLUMNS[table]['keys'])


def compare_keys(table, t1, t2):
    return ' AND '.join([f"{t1}.{c} = {t2}.{c}" for c in COLUMNS[table]['keys']])


# snapshot that processes both tables separately and combines the result with a union all
# (better suited for analytical queries)
snapshot = lambda tablename, sts, project=[], where=None: f"""
    ((SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}
    FROM {tablename}_storage s
    WHERE ({','.join(COLUMNS[tablename]['keys'])}) NOT IN (
        SELECT {','.join(COLUMNS[tablename]['keys'])}
        FROM {tablename}_cache c
        JOIN Log_committed ON Log_committed.txid = c.txid
        WHERE Log_committed.cts <= {sts}
            	{('AND ' + where) if where else ''}
    ) {('AND ' + where) if where else ''})
    UNION ALL
    (SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}
    FROM (
        SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}, deleted + 0 as deleted
        FROM {tablename}_cache c
        JOIN Log_committed ON Log_committed.txid = c.txid
        WHERE ({','.join(COLUMNS[tablename]['keys'])}, cts) IN (
            SELECT {','.join(COLUMNS[tablename]['keys'])}, max(cts)
            FROM {tablename}_cache c
            JOIN Log_committed ON Log_committed.txid = c.txid
            WHERE cts <= {sts}
                {('AND ' + where) if where else ''}
            GROUP BY {','.join(COLUMNS[tablename]['keys'])}
        )
            {('AND ' + where) if where else ''}
    ) _t WHERE NOT deleted
    )) {tablename}_snap
"""

# snapshot that processes both tables together, filtering the correct versions with a ranking function
# (better suited for operational queries)
# project - columns to project (empty list for all columns)
snapshot2 = lambda tablename, sts, project=[]: f"""
(
    select {','.join(COLUMNS[tablename]['all'] if project == [] else project)} 
    from (
        select *, rank() over (partition by {','.join(COLUMNS[tablename]['keys'])} order by cts desc) as rank
        from (
            (SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}, false as deleted, 0 as cts
            FROM {tablename}_storage)
            UNION ALL
            (SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}, deleted, cts + 0 as cts
            FROM {tablename}_cache
            JOIN Log_committed ON Log_committed.txid = {tablename}_cache.txid)
        ) t1
        WHERE cts <= {sts}
    ) t2 where not deleted and rank = 1
) {tablename}_snap
"""

# like snapshot2 but able to manually push filters (when the filter is different than the partition key)
snapshot3 = lambda tablename, sts, project=[], where="": f"""
(
    select {','.join(COLUMNS[tablename]['all'] if project == [] else project)} 
    from (
        select *, rank() over (partition by {','.join(COLUMNS[tablename]['keys'])} order by cts desc) as rank
        from (
            (SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}, false as deleted, 0 as cts
            FROM {tablename}_storage {"" if where == "" else ("WHERE " + where)})
            UNION ALL
            (SELECT {','.join(COLUMNS[tablename]['all'] if project == [] else project)}, deleted, cts + 0 as cts
            FROM {tablename}_cache
            JOIN Log_committed ON Log_committed.txid = {tablename}_cache.txid
            {"" if where == "" else ("WHERE " + where)})
        ) t1
        WHERE cts <= {sts}
    ) t2 where not deleted and rank = 1
) {tablename}_snap
"""

TXN_QUERIES = {
    "DELIVERY": {
        "getNewOrder": lambda sts, txid=None: f"SELECT NO_O_ID FROM {snapshot3('NEW_ORDER', sts, ['NO_O_ID', 'pk'], 'pk_partial = ?')} LIMIT 1", #
        "deleteNewOrder": lambda sts, txid: f"INSERT INTO NEW_ORDER_cache (NO_D_ID, NO_W_ID, NO_O_ID, deleted, txid, pk, pk_partial) VALUES (?, ?, ?, true, {txid}, ?, ?)", # d_id, w_id, no_o_id
        "getCId": lambda sts, txid=None: f"SELECT O_C_ID FROM {snapshot2('ORDERS', sts, ['O_C_ID', 'pk'])} WHERE pk = ?", # no_o_id, d_id, w_id
        "updateOrders": lambda sts, txid: f"INSERT INTO ORDERS_cache SELECT O_ID, O_C_ID, O_D_ID, O_W_ID, O_ENTRY_D, ?, O_OL_CNT, O_ALL_LOCAL, pk, false, {txid} FROM {snapshot2('ORDERS', sts, ['O_W_ID', 'O_D_ID', 'O_ID', 'O_C_ID', 'O_ENTRY_D', 'O_OL_CNT', 'O_ALL_LOCAL', 'pk'])} WHERE pk = ?", # o_carrier_id, no_o_id, d_id, w_id
        "updateOrderLine": lambda sts, txid: f"INSERT INTO ORDER_LINE_cache SELECT OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, ?, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, pk, pk_partial, false, {txid} FROM {snapshot2('ORDER_LINE', sts, ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_QUANTITY', 'OL_AMOUNT', 'OL_DIST_INFO', 'pk', 'pk_partial'])} WHERE pk = ?", # o_entry_d, no_o_id, d_id, w_id
        "sumOLAmount": lambda sts, txid=None: f"SELECT SUM(OL_AMOUNT) FROM {snapshot2('ORDER_LINE', sts, ['OL_AMOUNT', 'pk'])} WHERE pk = ?", # no_o_id, d_id, w_id
        "updateCustomer": lambda sts, txid: f"INSERT INTO CUSTOMER_cache SELECT C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE + ?, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA, pk, pk_last, false, {txid} FROM {snapshot2('CUSTOMER', sts)} WHERE pk = ?", # ol_total, c_id, d_id, w_id
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": lambda sts, txid=None: f"SELECT W_TAX FROM {snapshot2('WAREHOUSE', sts, ['W_TAX', 'W_ID'])} WHERE W_ID = ?", # w_id
        "getDistrict": lambda sts, txid=None: f"SELECT D_TAX, D_NEXT_O_ID FROM {snapshot2('DISTRICT', sts, ['D_TAX', 'D_NEXT_O_ID', 'pk'])} WHERE pk = ?", # d_id, w_id
        "incrementNextOrderId": lambda sts, txid: f"INSERT INTO DISTRICT_cache SELECT D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, ?, pk, false, {txid} FROM {snapshot2('DISTRICT', sts, ['D_ID', 'D_W_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'pk'])} WHERE pk = ?", # d_next_o_id, d_id, w_id
        "getCustomer": lambda sts, txid=None: f"SELECT C_DISCOUNT, C_LAST, C_CREDIT FROM {snapshot2('CUSTOMER', sts, ['C_DISCOUNT', 'C_LAST', 'C_CREDIT', 'pk'])} WHERE pk = ?", # w_id, d_id, c_id
        "createOrder": lambda sts, txid: f"INSERT INTO ORDERS_cache (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL, deleted, txid, pk) VALUES (?, ?, ?, ?, ?, ?, ?, ?, false, {txid}, ?)", # d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local
        "createNewOrder": lambda sts, txid: f"INSERT INTO NEW_ORDER_cache (NO_O_ID, NO_D_ID, NO_W_ID, deleted, txid, pk, pk_partial) VALUES (?, ?, ?, false, {txid}, ?, ?)", # o_id, d_id, w_id
        "getItemInfo": lambda sts, txid=None: f"SELECT I_PRICE, I_NAME, I_DATA FROM ITEM WHERE I_ID = ?", # ol_i_id
        "getStockInfo": lambda sts, txid=None: f"SELECT S_QUANTITY, S_DATA, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DIST_%02d FROM {snapshot2('STOCK', sts, ['S_QUANTITY', 'S_DATA', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DIST_%02d', 'pk'])} WHERE pk = ?", # d_id, ol_i_id, ol_supply_w_id
        "updateStock": lambda sts, txid: f"INSERT INTO STOCK_cache SELECT S_I_ID, S_W_ID, ?, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10, ?, ?, ?, S_DATA, pk, false, {txid} FROM {snapshot2('STOCK', sts, ['S_I_ID', 'S_W_ID', 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_DATA', 'pk'])} WHERE pk = ?", # s_quantity, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id
        "createOrderLine": lambda sts, txid: f"INSERT INTO ORDER_LINE_cache (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_DELIVERY_D, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, deleted, txid, pk, pk_partial) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false, {txid}, ?, ?)", # o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info        
    },
    "ORDER_STATUS": {
        "getCustomerByCustomerId": lambda sts, txid=None: f"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM {snapshot2('CUSTOMER', sts, ['C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_BALANCE', 'pk'])} WHERE pk = ?", # w_id, d_id, c_id
        "getCustomersByLastName": lambda sts, txid=None: f"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE FROM {snapshot3('CUSTOMER', sts, ['C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_BALANCE', 'pk', 'pk_last'], 'pk_last = ?')} ORDER BY C_FIRST", # w_id, d_id, c_last
        "getLastOrder": lambda sts, txid=None: f"SELECT O_ID, O_CARRIER_ID, O_ENTRY_D FROM {snapshot2('ORDERS', sts, ['O_ID', 'O_CARRIER_ID', 'O_ENTRY_D', 'pk'])} WHERE pk = ? ORDER BY O_ID DESC LIMIT 1", # w_id, d_id, c_id
        "getOrderLines": lambda sts, txid=None: f"SELECT OL_SUPPLY_W_ID, OL_I_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D FROM {snapshot2('ORDER_LINE', sts, ['OL_SUPPLY_W_ID', 'OL_I_ID', 'OL_QUANTITY', 'OL_AMOUNT', 'OL_DELIVERY_D', 'pk'])} WHERE pk = ?", # w_id, d_id, o_id        
    },
    "PAYMENT": {
        "getWarehouse": lambda sts, txid=None: f"SELECT W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP FROM {snapshot2('WAREHOUSE', sts, ['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP'])} WHERE W_ID = ?", # w_id
        "updateWarehouseBalance": lambda sts, txid: f"insert into WAREHOUSE_cache select W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD + ?, false, {txid} from {snapshot2('WAREHOUSE', sts, ['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD'])} WHERE W_ID = ?",
        "getDistrict": lambda sts, txid=None: f"SELECT D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP FROM {snapshot2('DISTRICT', sts, ['D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'pk'])} WHERE pk = ?", # w_id, d_id
        "updateDistrictBalance": lambda sts, txid: f"insert into DISTRICT_cache select D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD + ?, D_NEXT_O_ID, pk, false, {txid} from {snapshot2('DISTRICT', sts)} WHERE pk = ?",
        "getCustomerByCustomerId": lambda sts, txid=None: f"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM {snapshot2('CUSTOMER', sts, ['C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT', 'C_BALANCE', 'C_YTD_PAYMENT', 'C_PAYMENT_CNT', 'C_DATA', 'pk'])} WHERE pk = ?", # w_id, d_id, c_id
        "getCustomersByLastName": lambda sts, txid=None: f"SELECT C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DATA FROM {snapshot3('CUSTOMER', sts, ['C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT', 'C_BALANCE', 'C_YTD_PAYMENT', 'C_PAYMENT_CNT', 'C_DATA', 'pk', 'pk_last'], 'pk_last = ?')} ORDER BY C_FIRST", # w_id, d_id, c_last
        "updateBCCustomer": lambda sts, txid: f"insert into CUSTOMER_cache select C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, cast(? as int), cast(? as int), cast(? as int), C_DELIVERY_CNT, ?, pk, pk_last, false, {txid} from {snapshot2('CUSTOMER', sts, ['C_ID', 'C_D_ID', 'C_W_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT', 'C_DELIVERY_CNT', 'pk', 'pk_last'])} WHERE pk = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id
        "updateGCCustomer": lambda sts, txid: f"insert into CUSTOMER_cache select C_ID, C_D_ID, C_W_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, cast(? as int), cast(? as int), cast(? as int), C_DELIVERY_CNT, C_DATA, pk, pk_last, false, {txid} from {snapshot2('CUSTOMER', sts, ['C_ID', 'C_D_ID', 'C_W_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_STREET_1', 'C_STREET_2', 'C_CITY', 'C_STATE', 'C_ZIP', 'C_PHONE', 'C_SINCE', 'C_CREDIT', 'C_CREDIT_LIM', 'C_DISCOUNT', 'C_DELIVERY_CNT', 'C_DATA', 'pk', 'pk_last'])} WHERE pk = ?", # c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id
        "insertHistory": lambda sts, txid: f"INSERT INTO HISTORY_cache VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, false, {txid})",
    },
    "STOCK_LEVEL": {
        "getOId": lambda sts, txid=None: f"SELECT D_NEXT_O_ID FROM {snapshot2('DISTRICT', sts, ['D_NEXT_O_ID', 'pk'])} WHERE pk = ?", 
        "getStockCount": lambda sts, txid=None: f"""
            SELECT COUNT(DISTINCT(OL_I_ID)) FROM (
                SELECT OL_I_ID, OL_O_ID + 1 - 1 as OL_O_ID FROM -- + 1 - 1 is used to force the filter on OL_O_ID to be the last one executed, as it results in a better plan
                {snapshot('ORDER_LINE', sts, ['OL_O_ID', 'OL_I_ID'], 'pk_partial = ?')}, {snapshot('STOCK', sts, ['S_I_ID', 'S_QUANTITY'], 'S_W_ID = ?')}
                    WHERE S_I_ID = OL_I_ID
                        AND S_QUANTITY < ?
                ) t
            WHERE OL_O_ID < ? 
                AND OL_O_ID >= ?
        """,
    },
    "SQLTXN": {
        "getTxidSts": lambda **kwargs: "select txid_sequence(0), sts_sequence('get', null)",
        "insertLogBegan": lambda **kwargs: "insert into log_began values (?, ?)",
        "currentSts": lambda **kwargs: "select get_value_for('sys', 'sts_sequence')",
        "acquireCtn": lambda **kwargs: "select ctn_sequence('acquire')",
        "releaseCtn": lambda **kwargs: "select ctn_sequence('release')",
        "insertLogCommitting": lambda **kwargs: "insert into log_committing values (?, ?)",
        "certify": lambda **kwargs: """
            SELECT count(*) > 0
            FROM Write_Sets WS
            WHERE pk IN (SELECT pk FROM Write_Sets WHERE txid = ?)
                AND txid <> ? -- do not match my write set
                AND (txid IN (SELECT txid FROM Log_committed WHERE cts > ?) -- transaction committed after mine started
                    OR (txid NOT IN (SELECT txid FROM Log_committed) -- transaction not committed,
                        AND txid IN (SELECT txid FROM Log_committing WHERE ctn < ?) -- committing before me,
                        AND txid NOT IN (SELECT txid FROM Log_aborted))) -- and not aborted
        """,
        "acquireCts": lambda **kwargs: "select cts_sequence(0)",
        "insertLogCommitted": lambda **kwargs: f"insert into log_committed values (?, (select cts_sequence(0)), ?)",
        "waitAndAdvanceSts": lambda **kwargs: f"select sts_sequence('waitAndAdvance', (select cts from log_committed where txid = ?))",
        "insertLogAborted": lambda **kwargs: "insert into log_aborted values (?, ?)",
    }
}


TXN_PREPARED_IDS = defaultdict(int)
TXN_PREPARED = {
    "DELIVERY": {
        "getNewOrder": lambda sts, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['DELIVERY-getNewOrder']}('{w_id}.{d_id}', '{w_id}.{d_id}', {sts})",
        "deleteNewOrder": lambda sts, txid, d_id, w_id, o_id : f"EXEC {TXN_PREPARED_IDS['DELIVERY-deleteNewOrder']}({d_id}, {w_id}, {o_id}, {txid}, '{w_id}.{d_id}.{o_id}', '{w_id}.{d_id}')",
        "getCId": lambda sts, o_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['DELIVERY-getCId']}({sts}, '{w_id}.{d_id}.{o_id}')",
        "updateOrders": lambda sts, txid, carrier_id, o_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['DELIVERY-updateOrders']}({carrier_id}, {txid}, {sts}, '{w_id}.{d_id}.{o_id}')",
        "updateOrderLine": lambda sts, txid, delivery_d, o_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['DELIVERY-updateOrderLine']}('{delivery_d}', {txid}, {sts}, '{w_id}.{d_id}.{o_id}')",
        "sumOLAmount": lambda sts, o_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['DELIVERY-sumOLAmount']}({sts}, '{w_id}.{d_id}.{o_id}')",
        "updateCustomer": lambda sts, txid, balance, c_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['DELIVERY-updateCustomer']}({balance}, {txid}, {sts}, '{w_id}.{d_id}.{c_id}')",
    },
    "NEW_ORDER": {
        "getWarehouseTaxRate": lambda sts, w_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-getWarehouseTaxRate']}({sts}, {w_id})",
        "getDistrict": lambda sts, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-getDistrict']}({sts}, '{w_id}.{d_id}')",
        "incrementNextOrderId": lambda sts, txid, next_o_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-incrementNextOrderId']}({next_o_id}, {txid}, {sts}, '{w_id}.{d_id}')",
        "getCustomer": lambda sts, w_id, d_id, c_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-getCustomer']}({sts}, '{w_id}.{d_id}.{c_id}')",
        "createOrder": lambda sts, txid, o_id, d_id, w_id, c_id, entry_d, carrier_id, ol_cnt, all_local: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-createOrder']}({o_id}, {d_id}, {w_id}, {c_id}, '{entry_d}', {carrier_id}, {ol_cnt}, {all_local}, {txid}, '{w_id}.{d_id}.{o_id}')",
        "createNewOrder": lambda sts, txid, o_id, d_id, w_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-createNewOrder']}({o_id}, {d_id}, {w_id}, {txid}, '{w_id}.{d_id}.{o_id}', '{w_id}.{d_id}')",
        "getItemInfo": lambda sts, i_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-getItemInfo']}({i_id})",
        "getStockInfo": lambda sts, d, i_id, w_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-getStockInfo%02d' % d]}({sts}, '{w_id}.{i_id}')",
        "updateStock": lambda sts, txid, s_quantity, s_ytd, s_order_cnt, s_remote_cnt, i_id, w_id: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-updateStock']}({s_quantity}, {s_ytd}, {s_order_cnt}, {s_remote_cnt}, {txid}, {sts}, '{w_id}.{i_id}')",
        "createOrderLine": lambda sts, txid, o_id, d_id, w_id, number, i_id, supply_w_id, delivery_d, quantity, amount, dist_info: f"EXEC {TXN_PREPARED_IDS['NEW_ORDER-createOrderLine']}({o_id}, {d_id}, {w_id}, {number}, {i_id}, {supply_w_id}, '{delivery_d}', {quantity}, {amount}, '{dist_info}', {txid}, '{w_id}.{d_id}.{o_id}', '{w_id}.{d_id}')",
    },
    "ORDER_STATUS": {
        "getCustomerByCustomerId": lambda sts, w_id, d_id, c_id: f"EXEC {TXN_PREPARED_IDS['ORDER_STATUS-getCustomerByCustomerId']}({sts}, '{w_id}.{d_id}.{c_id}')",
        "getCustomersByLastName": lambda sts, w_id, d_id, c_last: f"EXEC {TXN_PREPARED_IDS['ORDER_STATUS-getCustomersByLastName']}('{w_id}.{d_id}.{c_last}', '{w_id}.{d_id}.{c_last}', {sts})",
        "getLastOrder": lambda sts, w_id, d_id, c_id: f"EXEC {TXN_PREPARED_IDS['ORDER_STATUS-getLastOrder']}({sts}, '{w_id}.{d_id}.{c_id}')",
        "getOrderLines": lambda sts, w_id, d_id, o_id: f"EXEC {TXN_PREPARED_IDS['ORDER_STATUS-getOrderLines']}({sts}, '{w_id}.{d_id}.{o_id}')",
    },
    "PAYMENT": {
        "getWarehouse": lambda sts, w_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-getWarehouse']}({sts}, {w_id})",
        "updateWarehouseBalance": lambda sts, txid, w_ytd, w_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-updateWarehouseBalance']}({w_ytd}, {txid}, {sts}, {w_id})",
        "getDistrict": lambda sts, w_id, d_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-getDistrict']}({sts}, '{w_id}.{d_id}')",
        "updateDistrictBalance": lambda sts, txid, h_amount, w_id, d_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-updateDistrictBalance']}({h_amount}, {txid}, {sts}, '{w_id}.{d_id}')",
        "getCustomerByCustomerId": lambda sts, w_id, d_id, c_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-getCustomerByCustomerId']}({sts}, '{w_id}.{d_id}.{c_id}')",
        "getCustomersByLastName": lambda sts, w_id, d_id, c_last: f"EXEC {TXN_PREPARED_IDS['PAYMENT-getCustomersByLastName']}('{w_id}.{d_id}.{c_last}', '{w_id}.{d_id}.{c_last}', {sts})",
        "updateBCCustomer": lambda sts, txid, c_balance, c_ytd_payment, c_payment_cnt, c_data, w_id, d_id, c_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-updateBCCustomer']}({c_balance}, {c_ytd_payment}, {c_payment_cnt}, '{c_data}', {txid}, {sts}, '{w_id}.{d_id}.{c_id}')",
        "updateGCCustomer": lambda sts, txid, c_balance, c_ytd_payment, c_payment_cnt, w_id, d_id, c_id: f"EXEC {TXN_PREPARED_IDS['PAYMENT-updateGCCustomer']}({c_balance}, {c_ytd_payment}, {c_payment_cnt}, {txid}, {sts}, '{w_id}.{d_id}.{c_id}')",
        "insertHistory": lambda sts, txid, h_id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data: f"EXEC {TXN_PREPARED_IDS['PAYMENT-insertHistory']}('{h_id}', {h_c_id}, {h_c_d_id}, {h_c_w_id}, {h_d_id}, {h_w_id}, '{h_date}', {h_amount}, '{h_data}', {txid})",
    },
    "STOCK_LEVEL": {
        "getOId": lambda sts, w_id, d_id: f"EXEC {TXN_PREPARED_IDS['STOCK_LEVEL-getOId']}({sts}, '{w_id}.{d_id}')", 
        "getStockCount": lambda sts, ol_w_id, ol_d_id, o_id_max, o_id_min, s_w_id, s_quantity, : f"EXEC {TXN_PREPARED_IDS['STOCK_LEVEL-getStockCount']}({sts}, '{ol_w_id}.{ol_d_id}', '{ol_w_id}.{ol_d_id}', {sts}, '{ol_w_id}.{ol_d_id}', '{ol_w_id}.{ol_d_id}', {sts}, {s_w_id}, {s_w_id}, {sts}, {s_w_id}, {s_w_id}, {s_quantity}, {o_id_max}, {o_id_min})"
    },
    "SQLTXN": {
        "getTxidSts": lambda: f"EXEC {TXN_PREPARED_IDS['SQLTXN-getTxidSts']}()",
        "insertLogBegan": lambda txid, sts: f"EXEC {TXN_PREPARED_IDS['SQLTXN-insertLogBegan']}({txid}, {sts})",
        "currentSts":  lambda: f"EXEC {TXN_PREPARED_IDS['SQLTXN-currentSts']}()",
        "acquireCtn":  lambda: f"EXEC {TXN_PREPARED_IDS['SQLTXN-acquireCtn']}()",
        "releaseCtn":  lambda: f"EXEC {TXN_PREPARED_IDS['SQLTXN-releaseCtn']}()",
        "insertLogCommitting":  lambda txid, ctn: f"EXEC {TXN_PREPARED_IDS['SQLTXN-insertLogCommitting']}({txid}, {ctn})",
        "certify":  lambda txid, sts, ctn: f"EXEC {TXN_PREPARED_IDS['SQLTXN-certify']}({txid}, {txid}, {sts}, {ctn})",
        "acquireCts": lambda: f"EXEC {TXN_PREPARED_IDS['SQLTXN-acquireCts']}()",
        "insertLogCommitted": lambda txid, sts: f"EXEC {TXN_PREPARED_IDS['SQLTXN-insertLogCommitted']}({txid}, {sts})",
        "waitSts": lambda cts: f"EXEC {TXN_PREPARED_IDS['SQLTXN-waitSts']}({cts})",
        "advanceSts": lambda: f"EXEC {TXN_PREPARED_IDS['SQLTXN-advanceSts']}()",
        "waitAndAdvanceSts": lambda txid: f"EXEC {TXN_PREPARED_IDS['SQLTXN-waitAndAdvanceSts']}({txid})",
        "insertLogAborted": lambda txid, sts: f"EXEC {TXN_PREPARED_IDS['SQLTXN-insertLogAborted']}({txid}, {sts})",
    }
}


SCHEMA = f'''
    CREATE TABLE WAREHOUSE_STORAGE (
        W_ID SMALLINT DEFAULT '0' NOT NULL,
        W_NAME VARCHAR(16) DEFAULT NULL,
        W_STREET_1 VARCHAR(32) DEFAULT NULL,
        W_STREET_2 VARCHAR(32) DEFAULT NULL,
        W_CITY VARCHAR(32) DEFAULT NULL,
        W_STATE VARCHAR(2) DEFAULT NULL,
        W_ZIP VARCHAR(9) DEFAULT NULL,
        W_TAX decimal(18,4) DEFAULT NULL,
        W_YTD decimal(18,4) DEFAULT NULL
    );

    CREATE TABLE WAREHOUSE_CACHE (
        W_ID SMALLINT DEFAULT '0' NOT NULL,
        W_NAME VARCHAR(16) DEFAULT NULL,
        W_STREET_1 VARCHAR(32) DEFAULT NULL,
        W_STREET_2 VARCHAR(32) DEFAULT NULL,
        W_CITY VARCHAR(32) DEFAULT NULL,
        W_STATE VARCHAR(2) DEFAULT NULL,
        W_ZIP VARCHAR(9) DEFAULT NULL,
        W_TAX decimal(18,4) DEFAULT NULL,
        W_YTD decimal(18,4) DEFAULT NULL,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE DISTRICT_STORAGE (
        D_ID smallint DEFAULT '0' NOT NULL,
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
        pk varchar(16) not null
    );

    CREATE TABLE DISTRICT_CACHE (
        D_ID smallint DEFAULT '0' NOT NULL,
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
        pk varchar(16) not null,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE ITEM (
        I_ID INTEGER DEFAULT '0' NOT NULL,
        I_IM_ID INTEGER DEFAULT NULL,
        I_NAME VARCHAR(32) DEFAULT NULL,
        I_PRICE decimal(18,4) DEFAULT NULL,
        I_DATA VARCHAR(64) DEFAULT NULL
    );

    CREATE TABLE CUSTOMER_STORAGE (
        C_ID INTEGER DEFAULT '0' NOT NULL,
        C_D_ID smallint DEFAULT '0' NOT NULL,
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
        pk varchar(16) not null,
        pk_last varchar(22) not null
    );

    CREATE TABLE CUSTOMER_CACHE (
        C_ID INTEGER DEFAULT '0' NOT NULL,
        C_D_ID smallint DEFAULT '0' NOT NULL,
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
        pk varchar(16) not null,
        pk_last varchar(22) not null,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE HISTORY_STORAGE (
        H_ID VARCHAR(10) NOT NULL,
        H_C_ID INTEGER DEFAULT NULL,
        H_C_D_ID smallint DEFAULT NULL,
        H_C_W_ID SMALLINT DEFAULT NULL,
        H_D_ID smallint DEFAULT NULL,
        H_W_ID SMALLINT DEFAULT '0' NOT NULL,
        H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        H_AMOUNT decimal(18,4) DEFAULT NULL,
        H_DATA VARCHAR(32) DEFAULT NULL
    );

    CREATE TABLE HISTORY_CACHE (
        H_ID VARCHAR(10) NOT NULL,
        H_C_ID INTEGER DEFAULT NULL,
        H_C_D_ID smallint DEFAULT NULL,
        H_C_W_ID SMALLINT DEFAULT NULL,
        H_D_ID smallint DEFAULT NULL,
        H_W_ID SMALLINT DEFAULT '0' NOT NULL,
        H_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        H_AMOUNT decimal(18,4) DEFAULT NULL,
        H_DATA VARCHAR(32) DEFAULT NULL,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE STOCK_STORAGE (
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
        pk varchar(16) not null
    );

    CREATE TABLE STOCK_CACHE
     (
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
        pk varchar(16) not null,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE ORDERS_STORAGE (
        O_ID INTEGER DEFAULT '0' NOT NULL,
        O_C_ID INTEGER DEFAULT NULL,
        O_D_ID smallint DEFAULT '0' NOT NULL,
        O_W_ID SMALLINT DEFAULT '0' NOT NULL,
        O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        O_CARRIER_ID INTEGER DEFAULT NULL,
        O_OL_CNT INTEGER DEFAULT NULL,
        O_ALL_LOCAL INTEGER DEFAULT NULL,
        pk varchar(16) not null
    );

    CREATE TABLE ORDERS_CACHE (
        O_ID INTEGER DEFAULT '0' NOT NULL,
        O_C_ID INTEGER DEFAULT NULL,
        O_D_ID smallint DEFAULT '0' NOT NULL,
        O_W_ID SMALLINT DEFAULT '0' NOT NULL,
        O_ENTRY_D TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
        O_CARRIER_ID INTEGER DEFAULT NULL,
        O_OL_CNT INTEGER DEFAULT NULL,
        O_ALL_LOCAL INTEGER DEFAULT NULL,
        pk varchar(16) not null,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE NEW_ORDER_STORAGE (
        NO_O_ID INTEGER DEFAULT '0' NOT NULL,
        NO_D_ID smallint DEFAULT '0' NOT NULL,
        NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
        pk varchar(16) not null,
        pk_partial varchar(8) not null
    );

    CREATE TABLE NEW_ORDER_CACHE (
        NO_O_ID INTEGER DEFAULT '0' NOT NULL,
        NO_D_ID smallint DEFAULT '0' NOT NULL,
        NO_W_ID SMALLINT DEFAULT '0' NOT NULL,
        pk varchar(16) not null,
        pk_partial varchar(8) not null,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE TABLE ORDER_LINE_STORAGE (
        OL_O_ID INTEGER DEFAULT '0' NOT NULL,
        OL_D_ID smallint DEFAULT '0' NOT NULL,
        OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
        OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
        OL_I_ID INTEGER DEFAULT NULL,
        OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
        OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
        OL_QUANTITY INTEGER DEFAULT NULL,
        OL_AMOUNT decimal(18,4) DEFAULT NULL,
        OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
        pk varchar(16) not null,
        pk_partial varchar(10) not null
    );

    CREATE TABLE ORDER_LINE_CACHE (
        OL_O_ID INTEGER DEFAULT '0' NOT NULL,
        OL_D_ID smallint DEFAULT '0' NOT NULL,
        OL_W_ID SMALLINT DEFAULT '0' NOT NULL,
        OL_NUMBER INTEGER DEFAULT '0' NOT NULL,
        OL_I_ID INTEGER DEFAULT NULL,
        OL_SUPPLY_W_ID SMALLINT DEFAULT NULL,
        OL_DELIVERY_D TIMESTAMP DEFAULT NULL,
        OL_QUANTITY INTEGER DEFAULT NULL,
        OL_AMOUNT decimal(18,4) DEFAULT NULL,
        OL_DIST_INFO VARCHAR(32) DEFAULT NULL,
        pk varchar(16) not null,
        pk_partial varchar(10) not null,
        deleted bool default false not null,
        -- txid bigint default 0 not null,
        txid bigint default 0 NOT NULL
    );

    CREATE UNLOGGED TABLE Log_began (
        txid bigint,
        sts bigint
    );

    CREATE UNLOGGED TABLE Log_committing (
        txid bigint,
        ctn bigint
    );

    CREATE TABLE Log_committed (
        txid bigint,
        cts bigint,
        sts bigint
    );

    CREATE UNLOGGED TABLE Log_aborted (
        txid bigint,
        sts bigint
    );

    CREATE UNLOGGED TABLE Write_Sets (
        pk bigint not null,
        txid bigint not null
    );

    -- CHBENCH TABLES
    CREATE TABLE NATION (
        n_nationkey integer NOT NULL,
        n_name character(25) NOT NULL,
        n_regionkey integer NOT NULL,
        n_comment character(152) NOT NULL
    );

    CREATE TABLE REGION (
        r_regionkey integer NOT NULL,
        r_name character(55) NOT NULL,
        r_comment character(152) NOT NULL
    );

    CREATE TABLE SUPPLIER (
        su_suppkey integer NOT NULL,
        su_name character(25) NOT NULL,
        su_address character varying(40) NOT NULL,
        su_nationkey integer NOT NULL,
        su_phone character(15) NOT NULL,
        su_acctbal numeric(12,2) NOT NULL,
        su_comment character(101) NOT NULL
    );

    -- sts sequence
    create or replace function sts_sequence(cmd string, value integer)
    returns integer
    language c {{
        #include <string.h>
        #include <pthread.h>

        static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
        static long long int current_seq = 1;
        result->initialize(result, 1);
        char *cmd_c = cmd.data[0];
        long long int value_c = value.data[0];

        if (!strcmp(cmd_c, "init")) {{
            pthread_mutex_lock(&lock);
            current_seq = value_c;
            result->data[0] = current_seq;
            pthread_mutex_unlock(&lock);
        }}

        else if (!strcmp(cmd_c, "wait")) {{
            pthread_mutex_lock(&lock);
            while (value_c != current_seq + 1) {{
                pthread_cond_wait(&cond, &lock);
            }}
            result->data[0] = current_seq;
            pthread_mutex_unlock(&lock);
        }}

        else if (!strcmp(cmd_c, "advance")) {{
            pthread_mutex_lock(&lock);
            current_seq++;
            result->data[0] = current_seq;
            pthread_cond_broadcast(&cond);
            pthread_mutex_unlock(&lock);
        }}

        else if (!strcmp(cmd_c, "waitAndAdvance")) {{
            pthread_mutex_lock(&lock);
            while (value_c != current_seq + 1) {{
                pthread_cond_wait(&cond, &lock);
            }}
            current_seq++;
            result->data[0] = current_seq;
            pthread_cond_broadcast(&cond);
            pthread_mutex_unlock(&lock);
        }}

        else if (!strcmp(cmd_c, "get")) {{
            pthread_mutex_lock(&lock);
            result->data[0] = current_seq;
            pthread_mutex_unlock(&lock);
        }}
    }};

    -- txid sequence
    create or replace function txid_sequence(value bigint)
    returns integer
    language c {{
        #include <string.h>
        #include <pthread.h>

        static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        static long long int current_seq = 1;
        long long int value_c = value.data[0];
        result->initialize(result, 1);

        pthread_mutex_lock(&lock);
        if (value_c) {{
            current_seq = value_c;
            result->data[0] = current_seq;
        }}
        else {{
            result->data[0] = current_seq;
            current_seq++;
        }}
        pthread_mutex_unlock(&lock);
    }};

    -- ctn sequence
    create or replace function ctn_sequence(cmd string)
    returns integer
    language c {{
        #include <string.h>
        #include <pthread.h>

        static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        static long long int current_seq = 1;
        char *cmd_c = cmd.data[0];
        result->initialize(result, 1);
        
        // acquire lock and ctn but do not release it
        if (!strcmp(cmd_c, "acquire")) {{
            pthread_mutex_lock(&lock);
            result->data[0] = current_seq;
        }}
        
        // release the lock
        else if (!strcmp(cmd_c, "release")) {{
            current_seq++;
            pthread_mutex_unlock(&lock);
        }}
        
        // reset
        else if (!strcmp(cmd_c, "reset")) {{
            pthread_mutex_lock(&lock);
            current_seq = 1;
            pthread_mutex_unlock(&lock);
        }}
    }};

    -- cts sequence
    create or replace function cts_sequence(value bigint)
    returns integer
    language c {{
        #include <string.h>
        #include <pthread.h>

        static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
        static long long int current_seq = 1;
        long long int value_c = value.data[0];
        result->initialize(result, 1);

        pthread_mutex_lock(&lock);
        if (value_c) {{
            current_seq = value_c;
            result->data[0] = current_seq;
        }}
        else {{
            result->data[0] = current_seq;
            current_seq++;
        }}
        pthread_mutex_unlock(&lock);
    }};


    -- flush function
    create or replace function flush()
    returns bigint
    begin
        declare stable_sts bigint;

        -- get stable sts
        select sts into stable_sts 
        from (
            select sts
            from(
                select sts_sequence('get', null) as sts
                union all
                select min(sts) as sts
                from log_began 
                where txid not in (
                    select txid 
                    from log_committed
                    union all 
                    select txid 
                    from log_aborted)
            ) as t1
            where sts is not null
            order by sts
            limit 1
        ) as t2;

        -- delete obsolete data from all storage tables
''' + '\n'.join([f'''
        delete
        from {tablename}_storage
        where ({','.join(COLUMNS[tablename]['keys'])}) in (
            select {','.join(COLUMNS[tablename]['keys'])}
            from (
                select {','.join(COLUMNS[tablename]['keys'])}, rank() over (partition by {','.join(COLUMNS[tablename]['keys'])} order by cts desc) as rank
                from {tablename}_cache
                join log_committed on log_committed.txid = {tablename}_cache.txid
                where cts <= stable_sts
            ) as t
            where rank = 1
        );
''' for tablename in COLUMNS.keys() if tablename != 'ITEM']) + '''

        -- move latest stable cache data to the respective storage
''' + '\n'.join([f'''
        insert into {tablename}_storage ({','.join(COLUMNS[tablename]['all'])})
        select {','.join(COLUMNS[tablename]['all'])}
        from (
            select {','.join(COLUMNS[tablename]['all'])}, deleted, rank() over (partition by {','.join(COLUMNS[tablename]['keys'])} order by cts desc) as rank
            from {tablename}_cache
            join log_committed on log_committed.txid = {tablename}_cache.txid
            where cts <= stable_sts
        ) as t 
        where rank = 1 and not deleted;
''' for tablename in COLUMNS.keys() if tablename != 'ITEM']) + '''

        -- delete the stable data from the cache tables
        -- delete obsolete versions first to prevent inconsistent reads 
        -- (if we remove the most recent version first, a transactions can read the previous version over the data written in the cache)
''' + '\n'.join([f'''
        delete
        from {tablename}_cache
        where ({','.join(COLUMNS[tablename]['keys'])}, txid) in (
            select {','.join(COLUMNS[tablename]['keys'])}, txid
            from (
                select {','.join(COLUMNS[tablename]['keys'])}, {tablename}_cache.txid, rank() over (partition by {','.join(COLUMNS[tablename]['keys'])} order by cts desc) as rank
                from {tablename}_cache
                join log_committed on log_committed.txid = {tablename}_cache.txid
                where cts <= stable_sts
            ) as t
            where rank > 1
        );

        -- can now delete the most recent versions safely
        delete
        from {tablename}_cache
        where ({','.join(COLUMNS[tablename]['keys'])}, txid) in (
            select {','.join(COLUMNS[tablename]['keys'])}, {tablename}_cache.txid
            from {tablename}_cache
            join log_committed on log_committed.txid = {tablename}_cache.txid
            where cts <= stable_sts
        );
''' for tablename in COLUMNS.keys() if tablename != 'ITEM']) + '''

        return stable_sts;
    end;
'''


## ==============================================
## TiqueDriver
## ==============================================
class TiqueDriver(AbstractDriver):
    DEFAULT_CONFIG = {
        "driver": ("The odbc driver used", "/usr/lib/x86_64-linux-gnu/libMonetODBC.so" ),
        "host": ("The server address", "localhost"),
        "port": ("The server port", "50000"),
        "database": ("The database used", "tpcc"),
        "username": ("The username", "monetdb"),
        "password": ("The password", "monetdb")
    }
    

    def __init__(self, ddl):
        super(TiqueDriver, self).__init__("monetdb", ddl)
    
    ## ----------------------------------------------
    ## makeDefaultConfig
    ## ----------------------------------------------
    def makeDefaultConfig(self):
        return TiqueDriver.DEFAULT_CONFIG
    
    ## ----------------------------------------------
    ## loadConfig
    ## ----------------------------------------------
    def loadConfig(self, config):
        for key in TiqueDriver.DEFAULT_CONFIG.keys():
            assert key in config, "Missing parameter '%s' in %s configuration" % (key, self.name)
        
        self.database = str(config["database"])
        self.warehouses = config['warehouses']
        self.scale = config['scalefactor']
        self.conn = pyodbc.connect(
            'DRIVER={' + config['driver'] + 
            '};HOST=' + config['host'] + 
            ';PORT=' + config['port'] + 
            ';DATABASE=' + config['database'] + 
            ';UID=' + config['username'] + 
            ';PWD=' + config['password']
        )
        self.cursor = self.conn.cursor()
        self.conn.autocommit = True
        self.cursor.execute("set optimizer='minimal_pipe'")

        if config['reset']:
            self.createDb()

        if config['load_from_csv']:
            self.loadFromCsv()

        if not config['reset'] and not config['execute']:
            self.setupMetadata()
            if config['soft_reset']:
                self.cleanCacheTables()

        self.setTablesAsReadUncommitted()
        self.prepareStatements()


    def createDb(self):
        print('Creating database')
        tables = ['item', 'warehouse_storage', 'new_order_storage', 'orders_storage', 'order_line_storage', 
                  'customer_storage', 'district_storage', 'stock_storage', 'history_storage',
                  'warehouse_cache', 'new_order_cache', 'orders_cache', 'order_line_cache', 
                  'customer_cache', 'district_cache', 'stock_cache', 'history_cache',
                  'nation', 'region', 'supplier',
                  'write_sets', 'log_began', 'log_committing', 'log_committed', 'log_aborted']
        functions = ['flush']

        for func in functions:
            try:
                self.cursor.execute(f'DROP FUNCTION {func}')
            except:
                pass

        for table in tables:
            try:
                self.cursor.execute('DROP TABLE ' + table)
            except:
                pass

        self.cursor.execute(SCHEMA)
        self.cursor.execute("insert into log_committed values (0, 0, 0)")
        self.cursor.execute(f"select sts_sequence('init', 1)")
        self.cursor.execute(f"select txid_sequence(1)")
        self.cursor.execute(f"select ctn_sequence('reset')")
        self.cursor.execute(f"select cts_sequence(2)")
        
    
    def importCsv(self, tablename, filename):
        self.cursor.execute(f'''
            COPY INTO {tablename} 
            FROM '{filename}' 
            USING DELIMITERS '|', E'\n', '\"' NULL AS 'None'
        ''')


    def loadFromCsv(self):
        print('Loading from csv')
        tables = ['new_order', 'orders', 'order_line', 'customer', 'district', 'stock', 'warehouse', 
                  'history', 'item']
        data_folder = f'tpcc_data_{self.warehouses}_{self.scale}_single_pk'

        for table in tables:
            filename = os.path.join(os.path.abspath(os.getcwd()), data_folder, table + '.csv')
            try:
                if table == 'item':
                    self.importCsv(table, filename)
                else:
                    self.importCsv(table + '_storage', filename)
            except Exception as e:
                print(e)
                exit(f'Csv file not found: {filename}')
        
        tables_ch = ['nation', 'region', 'supplier']
        for table in tables_ch:
            filename = os.path.join(os.path.abspath(os.getcwd()), 'chbench_data', table + '.csv')
            try:
                self.importCsv(table, filename)
            except Exception as e:
                print(f'Warning: ch-bench csv file not found: {filename}')
        
        self.conn.commit()


    def setTablesAsReadUncommitted(self):
        tables = ['warehouse_storage', 'new_order_storage', 'orders_storage', 'order_line_storage', 
                  'customer_storage', 'district_storage', 'stock_storage', 'history_storage',
            'warehouse_cache', 'new_order_cache', 'orders_cache', 'order_line_cache', 'customer_cache', 
            'district_cache', 'stock_cache', 'history_cache', 'item', 'nation', 'supplier', 'region',
            'write_sets', 'log_began', 'log_committing', 'log_committed', 'log_aborted']
        
        for table in tables:
            self.cursor.execute(f"call set_table_as_read_uncommitted('sys', '{table}')")
        

    def prepareStatements(self):
        i = 0
        for procedure in sorted(TXN_QUERIES):
            for statement_name, statement in sorted(TXN_QUERIES[procedure].items()):
                # special getStockInfo procedure, must create one procedure for each district
                if statement_name == 'getStockInfo':
                    for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                        self.cursor.execute('PREPARE ' + statement(sts='$', txid='$').replace('?', '$') % (d_id, d_id, d_id, d_id))
                        TXN_PREPARED_IDS[procedure + '-' + ('getStockInfo%02d' % d_id)] = i
                        i += 1
                else:
                    self.cursor.execute('PREPARE ' + statement(sts='$', txid='$').replace('?', '$'))
                    TXN_PREPARED_IDS[procedure + '-' + statement_name] = i
                    i += 1


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


    # setup metadata between runs
    def setupMetadata(self):
        metadata_tables = ['write_sets', 'log_began', 'log_committing', 'log_aborted']
        for table in metadata_tables:
            self.cursor.execute(f'delete from {table}')

        self.cursor.execute('select max(txid), max(cts) from log_committed')
        txid, cts = self.cursor.fetchone()
        cts = cts if cts > 0 else 1

        self.cursor.execute(f"select sts_sequence('init', {cts})")
        self.cursor.execute(f"select txid_sequence({txid + 1})")
        self.cursor.execute(f"select cts_sequence({cts + 1})")
        self.cursor.execute(f"select ctn_sequence('reset')")


    # clean cache tables between runs (prevents having to repopulate)
    def cleanCacheTables(self):
        tables = ['warehouse_cache', 'new_order_cache', 'orders_cache', 'order_line_cache', 
                  'customer_cache', 'district_cache', 'stock_cache', 'history_cache']
        for table in tables:
            self.cursor.execute(f'truncate {table}')


    ## ----------------------------------------------
    ## loadFinish
    ## ----------------------------------------------
    def loadFinish(self):
        self.conn.autocommit = True


    # starts a transaction
    # returns txid, sts
    def begin(self):
        # get sts and txid
        self.cursor.execute(TXN_PREPARED['SQLTXN']['getTxidSts']())
        txid, sts = self.cursor.fetchone()

        # insert log began
        self.cursor.execute(TXN_PREPARED['SQLTXN']['insertLogBegan'](txid, sts))

        return txid, sts


    def strToBigint(self, s):
        return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16) % 18446744073709551614 - 9223372036854775807


    # commits a transaction
    # raises an exception if it fails
    def commit(self, txid, sts, write_set, write_statements):
        if write_set == [] or write_set == None:
            return self.commit_readonly(txid, sts)

        #begin_t = time.time()

        # add write set
        #t = time.time()
        ws_query = "insert into write_sets values " + ",".join([f"({self.strToBigint(ws[0])}, {txid})" for ws in write_set])
        self.cursor.execute(ws_query)
        #print(f'COMMIT_INSERT_WRITE_SET,{time.time() - t}')

        # acquire ctn
        self.cursor.execute(TXN_PREPARED['SQLTXN']['acquireCtn']())
        ctn = self.cursor.fetchone()[0]

        # update log
        #t = time.time()
        self.cursor.execute(TXN_PREPARED['SQLTXN']['insertLogCommitting'](txid, ctn))
        #print(f'COMMIT_UPDATE_LOG_CTN,{time.time() - t}')
        
        # release ctn lock
        self.cursor.execute(TXN_PREPARED['SQLTXN']['releaseCtn']())

        # check conflicts
        #t = time.time()
        self.cursor.execute(TXN_PREPARED['SQLTXN']['certify'](txid, sts, ctn))
        has_conflicts = self.cursor.fetchone()[0]
        #print(f'COMMIT_CHECK_CONFLICTS,{time.time() - t}')

        if has_conflicts:
            # abort
            self.abort(txid, sts)
        else:
            # execute statements
            #t = time.time()
            # combine all writes
            write_statements_exec = ';'.join([statement(sts, txid, *args) for statement, args in write_statements])
            # combine with acquire cts and insert into log
            write_statements_exec += ';' + TXN_PREPARED['SQLTXN']['insertLogCommitted'](txid, sts)
            self.cursor.execute(write_statements_exec)
            #print(f"COMMIT_EXEC_WRITES,{time.time() - t}")

            # wait and advance sts
            #t = time.time()
            self.cursor.execute(TXN_PREPARED['SQLTXN']['waitAndAdvanceSts'](txid))
            #print(f"COMMIT_WAIT_AND_ADVANCE_STS,{time.time() - t}")

            #print(f'COMMIT_TOTAL,{time.time() - begin_t}')


    def commit_readonly(self, txid, sts):
        # even though it is a read-only transaction that committed, we insert it into the aborted table since it is unlogged
        self.cursor.execute(TXN_PREPARED['SQLTXN']['insertLogAborted'](txid, sts))


    # aborts a transaction
    def abort(self, txid, sts):
        self.cursor.execute(TXN_PREPARED['SQLTXN']['insertLogAborted'](txid, sts))
        raise Exception(f'Transaction aborted (txid: {txid})')


    ## ----------------------------------------------
    ## doDelivery
    ## ----------------------------------------------
    def doDelivery(self, params):
        try:
            #begin = time.time()
            txid, sts = self.begin()
            q = TXN_PREPARED["DELIVERY"]
            ws = []
            write_statements = []

            w_id = params["w_id"]
            o_carrier_id = params["o_carrier_id"]
            ol_delivery_d = params["ol_delivery_d"]

            result = [ ]
            for d_id in range(1, constants.DISTRICTS_PER_WAREHOUSE+1):
                self.cursor.execute(q["getNewOrder"](sts, d_id, w_id))
                newOrder = self.cursor.fetchone()
                if newOrder == None:
                    ## No orders for this district: skip it. Note: This must be reported if > 1%
                    continue
                assert len(newOrder) > 0
                no_o_id = newOrder[0]
                
                self.cursor.execute(q["getCId"](sts, no_o_id, d_id, w_id))
                c_id = self.cursor.fetchone()[0]
                
                self.cursor.execute(q["sumOLAmount"](sts, no_o_id, d_id, w_id))
                ol_total = self.cursor.fetchone()[0]

                #self.cursor.execute(q["deleteNewOrder"], [d_id, w_id, no_o_id])
                write_statements.append((q["deleteNewOrder"], [d_id, w_id, no_o_id]))
                ws.append((f"NO.{w_id}.{d_id}.{no_o_id}",))

                #self.cursor.execute(q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id])
                write_statements.append((q["updateOrders"], [o_carrier_id, no_o_id, d_id, w_id]))
                ws.append((f"O.{w_id}.{d_id}.{no_o_id}",))

                #self.cursor.execute(q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id])
                write_statements.append((q["updateOrderLine"], [ol_delivery_d, no_o_id, d_id, w_id]))
                ws.append((f"OL.{w_id}.{d_id}.{no_o_id}",))

                # These must be logged in the "result file" according to TPC-C 2.7.2.2 (page 39)
                # We remove the queued time, completed time, w_id, and o_carrier_id: the client can figure
                # them out
                # If there are no order lines, SUM returns null. There should always be order lines.
                assert ol_total != None, "ol_total is NULL: there are no order lines. This should not happen"
                assert ol_total > 0.0

                #self.cursor.execute(q["updateCustomer"], [ol_total, c_id, d_id, w_id])
                write_statements.append((q["updateCustomer"], [ol_total, c_id, d_id, w_id]))
                ws.append((f"C.{w_id}.{d_id}.{c_id}",))

                result.append((d_id, no_o_id))
            ## FOR

            #print(f'DELIVERY_EXEC,{time.time() - begin}')

            self.commit(txid, sts, ws, write_statements)
            #print(f'DELIVERY_TOTAL,{time.time() - begin}')

            return result
        except Exception as e:
            #traceback.print_exc()
            raise e


    ## ----------------------------------------------
    ## doNewOrder
    ## ----------------------------------------------
    def doNewOrder(self, params):
        try:
            #begin = time.time()
            txid, sts = self.begin()
            #print(f'BEGIN,{time.time() - begin}')
            #begin = time.time()
            q = TXN_PREPARED["NEW_ORDER"]
            ws = []
            write_statements = []
            
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
            #b_for = time.time()
            for i in range(len(i_ids)):
                ## Determine if this is an all local order or not
                all_local = all_local and i_w_ids[i] == w_id
                #b = time.time()
                self.cursor.execute(q["getItemInfo"](sts, i_ids[i]))
                #print(f'new_order_getItemInfo,{time.time() - b}')
                items.append(self.cursor.fetchone())
            assert len(items) == len(i_ids)
            #print(f'new_order_forItem,{time.time() - b_for}')
            
            ## TPCC defines 1% of neworder gives a wrong itemid, causing rollback.
            ## Note that this will happen with 1% of transactions on purpose.
            for item in items:
                if len(item) == 0:
                    self.cursor.execute('rollback')
                    return
            ## FOR
            
            ## ----------------
            ## Collect Information from WAREHOUSE, DISTRICT, and CUSTOMER
            ## ----------------
            #b = time.time()
            self.cursor.execute(q["getWarehouseTaxRate"](sts, w_id))
            w_tax = self.cursor.fetchone()[0]
            #print(f'new_order_getWarehouseTaxRate,{time.time() - b}')
            
            #b = time.time()
            self.cursor.execute(q["getDistrict"](sts, d_id, w_id))
            district_info = self.cursor.fetchone()
            #print(f'new_order_getDistrict,{time.time() - b}')
            d_tax = district_info[0]
            d_next_o_id = district_info[1]
            
            self.cursor.execute(q["getCustomer"](sts, w_id, d_id, c_id))
            customer_info = self.cursor.fetchone()
            c_discount = customer_info[0]

            ## ----------------
            ## Insert Order Information
            ## ----------------
            ol_cnt = len(i_ids)
            o_carrier_id = constants.NULL_CARRIER_ID
            
            #self.cursor.execute(q["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id])
            write_statements.append((q["incrementNextOrderId"], [d_next_o_id + 1, d_id, w_id]))
            ws.append((f"D.{w_id}.{d_id}",))

            #self.cursor.execute(q["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local])
            write_statements.append((q["createOrder"], [d_next_o_id, d_id, w_id, c_id, o_entry_d, o_carrier_id, ol_cnt, all_local]))
            ws.append((f"O.{w_id}.{d_id}.{d_next_o_id}",))
            
            #self.cursor.execute(q["createNewOrder"], [d_next_o_id, d_id, w_id])
            write_statements.append((q["createNewOrder"], [d_next_o_id, d_id, w_id]))
            ws.append((f"NO.{w_id}.{d_id}.{d_next_o_id}",))

            ## ----------------
            ## Insert Order Item Information
            ## ----------------
            item_data = [ ]
            total = 0
            #b_for = time.time()
            for i in range(len(i_ids)):
                ol_number = i + 1
                ol_supply_w_id = i_w_ids[i]
                ol_i_id = i_ids[i]
                ol_quantity = i_qtys[i]

                itemInfo = items[i]
                i_name = itemInfo[1]
                i_data = itemInfo[2]
                i_price = itemInfo[0]

                #b = time.time()
                self.cursor.execute(q["getStockInfo"](sts, d_id, ol_i_id, ol_supply_w_id))
                stockInfo = self.cursor.fetchone()
                #print(f'new_order_getStockInfo,{time.time() - b}')
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

                #self.cursor.execute(q["updateStock"], [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id])
                write_statements.append((q["updateStock"], [s_quantity, s_ytd, s_order_cnt, s_remote_cnt, ol_i_id, ol_supply_w_id]))
                ws.append((f"S.{ol_supply_w_id}.{ol_i_id}",))

                if i_data.find(constants.ORIGINAL_STRING) != -1 and s_data.find(constants.ORIGINAL_STRING) != -1:
                    brand_generic = 'B'
                else:
                    brand_generic = 'G'

                ## Transaction profile states to use "ol_quantity * i_price"
                ol_amount = ol_quantity * i_price
                total += ol_amount

                #self.cursor.execute(q["createOrderLine"], [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx])
                write_statements.append((q["createOrderLine"], [d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, o_entry_d, ol_quantity, ol_amount, s_dist_xx]))
                ws.append((f"OL.{w_id}.{d_id}.{d_next_o_id}.{ol_number}",))

                ## Add the info to be returned
                item_data.append( (i_name, s_quantity, brand_generic, i_price, ol_amount) )
            ## FOR

            #print(f'new_order_for,{time.time() - b_for}')

            #print(f'NEW_ORDER_EXEC,{time.time() - begin}')
            
            ## Comit!
            self.commit(txid, sts, ws, write_statements)
            #print(f'NEW_ORDER_TOTAL,{time.time() - begin}')

            ## Adjust the total for the discount
            #\ "c_discount:", c_discount, type(c_discount)
            #print "w_tax:", w_tax, type(w_tax)
            #print "d_tax:", d_tax, type(d_tax)
            total *= (1 - c_discount) * (1 + w_tax + d_tax)

            ## Pack up values the client is missing (see TPC-C 2.4.3.5)
            misc = [ (w_tax, d_tax, d_next_o_id, total) ]
            
            return [ customer_info, misc, item_data ]
        except Exception as e:
            #traceback.print_exc()
            raise e


    ## ----------------------------------------------
    ## doOrderStatus
    ## ----------------------------------------------
    def doOrderStatus(self, params):
        #begin = time.time()
        txid, sts = self.begin()

        q = TXN_PREPARED["ORDER_STATUS"]
        
        w_id = params["w_id"]
        d_id = params["d_id"]
        c_id = params["c_id"]
        c_last = params["c_last"]
        
        assert w_id, pformat(params)
        assert d_id, pformat(params)

        if c_id != None:
            self.cursor.execute(q["getCustomerByCustomerId"](sts, w_id, d_id, c_id))
            customer = self.cursor.fetchone()
        else:
            # Get the midpoint customer's id
            self.cursor.execute(q["getCustomersByLastName"](sts, w_id, d_id, c_last))
            all_customers = self.cursor.fetchall()
            assert len(all_customers) > 0
            namecnt = len(all_customers)
            index = int((namecnt-1)/2)
            customer = all_customers[index]
            c_id = customer[0]
        assert len(customer) > 0
        assert c_id != None

        self.cursor.execute(q["getLastOrder"](sts, w_id, d_id, c_id))
        order = self.cursor.fetchone()
        if order:
            self.cursor.execute(q["getOrderLines"](sts, w_id, d_id, order[0]))
            orderLines = self.cursor.fetchall()
        else:
            orderLines = [ ]

        self.commit(txid, sts, None, None)
        #print(f'ORDER_STATUS_TOTAL,{time.time() - begin}')

        return [ customer, order, orderLines ]


    ## ----------------------------------------------
    ## doPayment
    ## ----------------------------------------------  
    def doPayment(self, params):
        try:
            #begin = time.time()
            txid, sts = self.begin()
            #print(f'BEGIN,{time.time() - begin}')
            #begin = time.time()
            
            q = TXN_PREPARED["PAYMENT"]
            w_id = params["w_id"]
            d_id = params["d_id"]
            h_amount = params["h_amount"]
            c_w_id = params["c_w_id"]
            c_d_id = params["c_d_id"]
            c_id = params["c_id"]
            c_last = params["c_last"]
            h_date = params["h_date"]
            ws = []
            write_statements = []

            if c_id != None:
                #begin_read_customer = time.time()
                self.cursor.execute(q["getCustomerByCustomerId"](sts, w_id, d_id, c_id))
                customer = self.cursor.fetchone()
                #print(f'PAYMENT_READ_CUSTOMER,{time.time() - begin_read_customer}')
            else:
                # Get the midpoint customer's id
                #begin_read_customer = time.time()
                self.cursor.execute(q["getCustomersByLastName"](sts, w_id, d_id, c_last))
                all_customers = self.cursor.fetchall()
                #print(f'PAYMENT_READ_CUSTOMER,{time.time() - begin_read_customer}')
                assert len(all_customers) > 0
                namecnt = len(all_customers)
                index = int((namecnt-1)/2)
                customer = all_customers[index]
                c_id = customer[0]
            assert len(customer) > 0
            c_balance = float(customer[14]) - h_amount
            c_ytd_payment = float(customer[15]) + h_amount
            c_payment_cnt = customer[16] + 1
            c_data = customer[17]

            #begin_read_warehouse = time.time()
            self.cursor.execute(q["getWarehouse"](sts, w_id))
            warehouse = self.cursor.fetchone()
            #print(f'PAYMENT_READ_WAREHOUSE,{time.time() - begin_read_warehouse}')

            #begin_read_district = time.time()
            self.cursor.execute(q["getDistrict"](sts, w_id, d_id))
            district = self.cursor.fetchone()
            #print(f'PAYMENT_READ_DISTRICT,{time.time() - begin_read_district}')
            
            #self.cursor.execute(q["updateWarehouseBalance"](txid, sts), [h_amount, w_id])
            write_statements.append((q["updateWarehouseBalance"], [h_amount, w_id]))
            ws.append((f"W.{w_id}",))
            #self.cursor.execute(q["updateDistrictBalance"](txid, sts), [h_amount, w_id, d_id])
            write_statements.append((q["updateDistrictBalance"], [h_amount, w_id, d_id]))
            ws.append((f"D.{w_id}.{d_id}",))

            # Customer Credit Information
            if customer[11] == constants.BAD_CREDIT:
                newData = " ".join(map(str, [c_id, c_d_id, c_w_id, d_id, w_id, h_amount]))
                c_data = (newData + "|" + c_data)
                if len(c_data) > constants.MAX_C_DATA: c_data = c_data[:constants.MAX_C_DATA]
                #self.cursor.execute(q["updateBCCustomer"](txid, sts), [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id])
                write_statements.append((q["updateBCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_data, c_w_id, c_d_id, c_id]))
                ws.append((f"C.{c_w_id}.{c_d_id}.{c_id}",))
            else:
                c_data = ""
                #self.cursor.execute(q["updateGCCustomer"](txid, sts), [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id])
                write_statements.append((q["updateGCCustomer"], [c_balance, c_ytd_payment, c_payment_cnt, c_w_id, c_d_id, c_id]))
                ws.append((f"C.{c_w_id}.{c_d_id}.{c_id}",))

            # Concatenate w_name, four spaces, d_name
            h_data = "%s    %s" % (warehouse[0], district[0])
            h_id = ''.join([string.ascii_letters[randint(0, len(string.ascii_letters) - 1)] for _ in range(6)])
            # Create the history record
            write_statements.append((q["insertHistory"], [h_id, c_id, c_d_id, c_w_id, d_id, w_id, h_date, h_amount, h_data]))

            #print(f'PAYMENT_EXEC,{time.time() - begin}')
            
            #begin = time.time()
            self.commit(txid, sts, ws, write_statements)
            #print(f'PAYMENT_COMMIT,{time.time() - begin}')

            # TPC-C 2.5.3.3: Must display the following fields:
            # W_ID, D_ID, C_ID, C_D_ID, C_W_ID, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP,
            # D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1,
            # C_STREET_2, C_CITY, C_STATE, C_ZIP, C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM,
            # C_DISCOUNT, C_BALANCE, the first 200 characters of C_DATA (only if C_CREDIT = "BC"),
            # H_AMOUNT, and H_DATE.

            #print(f'PAYMENT_TOTAL,{time.time() - begin}')
            
            # Hand back all the warehouse, district, and customer data
            return [ warehouse, district, customer ]
        
        except Exception as e:
            #traceback.print_exc()
            raise e


    ## ----------------------------------------------
    ## doStockLevel
    ## ----------------------------------------------    
    def doStockLevel(self, params):
        #begin = time.time()
        q = TXN_PREPARED["STOCK_LEVEL"]
        txid, sts = self.begin()

        w_id = params["w_id"]
        d_id = params["d_id"]
        threshold = params["threshold"]
        
        self.cursor.execute(q["getOId"](sts, w_id, d_id))
        result = self.cursor.fetchone()
        assert result
        o_id = result[0]
        
        self.cursor.execute(q["getStockCount"](sts, w_id, d_id, o_id, (o_id - 20), w_id, threshold))
        result = self.cursor.fetchone()
        
        self.commit(txid, sts, None, None)
        #print(f'STOCK_LEVEL_TOTAL,{time.time() - begin}')
        
        return int(result[0])
        
## CLASS
