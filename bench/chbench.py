import traceback
import pyodbc
import time
import sys
from multiprocessing import Pool, Manager
import argparse


# schema keys
keys = {
    'WAREHOUSE': ['W_ID'],
    'DISTRICT': ['pk'],
    'ITEM': ['I_ID'],
    'CUSTOMER': ['pk'],
    'STOCK': ['pk'],
    'ORDERS': ['pk'],
    'NEW_ORDER': ['pk'],
    'ORDER_LINE': ['pk'],
    'NATION': ['n_nationkey'],
    'REGION': ['r_regionkey'],
    'SUPPLIER': ['su_suppkey']
}


keys_and_values = {
    'WAREHOUSE': ['W_ID', 'W_NAME', 'W_STREET_1', 'W_STREET_2', 'W_CITY', 'W_STATE', 'W_ZIP', 'W_TAX', 'W_YTD', 'pk'],
    'DISTRICT': ['D_W_ID', 'D_ID', 'D_NAME', 'D_STREET_1', 'D_STREET_2', 'D_CITY', 'D_STATE', 'D_ZIP', 'D_TAX', 'D_YTD', 'D_NEXT_O_ID', 'pk'],
    'ITEM': ['I_ID', 'I_IM_ID', 'I_NAME', 'I_PRICE', 'I_DATA', 'pk'],
    'CUSTOMER': ['C_W_ID','C_D_ID','C_ID','C_FIRST','C_MIDDLE','C_LAST','C_STREET_1','C_STREET_2','C_CITY','C_STATE','C_ZIP','C_PHONE','C_SINCE','C_CREDIT','C_CREDIT_LIM','C_DISCOUNT','C_BALANCE','C_YTD_PAYMENT','C_PAYMENT_CNT','C_DELIVERY_CNT','C_DATA', 'pk'],
    'STOCK': ['S_W_ID', 'S_I_ID', 'S_QUANTITY', 'S_DIST_01', 'S_DIST_02', 'S_DIST_03', 'S_DIST_04', 'S_DIST_05', 'S_DIST_06', 'S_DIST_07', 'S_DIST_08', 'S_DIST_09', 'S_DIST_10', 'S_YTD', 'S_ORDER_CNT', 'S_REMOTE_CNT', 'S_DATA', 'pk'],
    'ORDERS': ['O_W_ID','O_D_ID','O_ID','O_C_ID','O_ENTRY_D','O_CARRIER_ID','O_OL_CNT','O_ALL_LOCAL', 'pk'],
    'NEW_ORDER': ['NO_W_ID','NO_D_ID','NO_O_ID', 'pk'],
    'ORDER_LINE': ['OL_W_ID', 'OL_D_ID', 'OL_O_ID', 'OL_NUMBER', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_DELIVERY_D', 'OL_QUANTITY', 'OL_AMOUNT', 'OL_DIST_INFO', 'pk'],
    'NATION': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment', 'pk'],
    'REGION': ['r_regionkey', 'r_name', 'r_comment', 'pk'],
    'SUPPLIER': ['su_suppkey', 'su_name', 'su_address', 'su_nationkey', 'su_phone', 'su_acctbal', 'su_comment', 'pk']
}


# snapshots

snapshot_native = lambda tablename, sts=0: f"""{tablename} snap_{tablename}"""

snapshot_cache_storage_union = lambda tablename, sts=0: f"""
    ((SELECT {','.join([col for col in keys_and_values[tablename]])}
    FROM {tablename}_storage s
    WHERE ({','.join(keys[tablename])}) NOT IN (
        SELECT {','.join(keys[tablename])} || ''
        FROM {tablename}_cache c
        JOIN Log_Committed on Log_Committed.txid = c.txid
        WHERE cts <= {sts}
    ))
    UNION ALL
    (SELECT {','.join([col for col in keys_and_values[tablename]])}
    FROM {tablename}_cache c
    JOIN Log_Committed on Log_Committed.txid = c.txid
    WHERE ({','.join(keys[tablename])}, cts) IN (
        SELECT {','.join(keys[tablename])} || '', max(cts)
        FROM {tablename}_cache c
        JOIN Log_Committed on Log_Committed.txid = c.txid
        WHERE cts <= {sts}
        GROUP BY {','.join(keys[tablename])}
    )
        AND NOT deleted
    )) snap_{tablename}"""

snapshot_cache_storage_window = lambda tablename, sts=0: f"""
(
    select *
    from (
        select *, rank() over (partition by {','.join(keys[tablename])} order by cts desc) as rank
        from (
            (SELECT *, false as deleted, 0 as cts
            FROM {tablename}_storage)
            UNION ALL
            (SELECT *
            FROM {tablename}_cache c
            JOIN Log_Committed on Log_Committed.txid = c.txid)
        ) t1
        where cts <= {sts}
    ) t2 where not deleted and rank = 1
) snap_{tablename}"""

snapshot_cache_storage_fulljoin = lambda tablename, sts=0: f"""
(
    select {','.join([f'(case when c.{col} is null then s.{col} else c.{col} end) as {col}' for col in keys_and_values[tablename]])}
    from {tablename}_storage s
    full join (
        select {','.join([col for col in keys_and_values[tablename]])}, deleted 
        from (
            select {','.join([col for col in keys_and_values[tablename]])}, deleted, rank() over(partition by {','.join(keys[tablename])} order by cts desc) as rank
            from {tablename}_cache c
            JOIN Log_Committed on Log_Committed.txid = c.txid
            where cts <= {sts}
        ) t
        where rank = 1
    ) c on {' AND '.join([f's.{pk} = c.{pk}' for pk in keys[tablename]])}
        where not deleted or deleted is null
) snap_{tablename}"""


# queries

queries_olap = lambda snapshot_type: [
    # 1
    lambda sts=0: f"""
        select   ol_number,
            sum(ol_quantity) as sum_qty,
            sum(ol_amount) as sum_amount,
            avg(ol_quantity) as avg_qty,
            avg(ol_amount) as avg_amount,
            count(*) as count_order
        from  {snapshot_type('ORDER_LINE', sts)}
        where  ol_delivery_d > '2007-01-02 00:00:00.000000'
        group by ol_number order by ol_number;
    """,

    # 2
    lambda sts=0: f"""
    select       su_suppkey, su_name, n_name, i_id, i_name, su_address, su_phone, su_comment
        from     {snapshot_native('ITEM', sts)}
        join {snapshot_type('STOCK', sts)} on s_i_id = i_id
        join {snapshot_native('SUPPLIER', sts)} on mod((s_w_id * s_i_id), 10000) = su_suppkey
        join {snapshot_native('NATION', sts)} on su_nationkey = n_nationkey
        join {snapshot_native('REGION', sts)} on n_regionkey = r_regionkey
        join (select s_i_id as m_i_id,
                min(s_quantity) as m_s_quantity
                from         {snapshot_type('STOCK', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_native('NATION', sts)}, {snapshot_native('REGION', sts)}
            where    mod((s_w_id*s_i_id),10000)=su_suppkey
                and su_nationkey=n_nationkey
                and n_regionkey=r_regionkey
                and r_name like 'Europ%'
            group by s_i_id) m on m_i_id = i_id
    where i_data like '%b'
        and r_name like 'Europ%'
        and s_quantity = m_s_quantity
    order by n_name, su_name, i_id
    """,

    # 3
    lambda sts=0: f"""
    select   ol_o_id, ol_w_id, ol_d_id,
        sum(ol_amount) as revenue, o_entry_d
    from   {snapshot_type('CUSTOMER', sts)}, {snapshot_type('NEW_ORDER', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_type('ORDER_LINE', sts)}
    where   c_state like 'a%'
        and c_id = o_c_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and no_w_id = o_w_id
        and no_d_id = o_d_id
        and no_o_id = o_id
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and o_entry_d > '2007-01-02 00:00:00.000000'
    group by ol_o_id, ol_w_id, ol_d_id, o_entry_d
    order by revenue desc, o_entry_d;
    """,

    # 4
    lambda sts=0: f"""
    select o_ol_cnt, count(*) as order_count
    from {snapshot_type('ORDERS', sts)}
    where o_entry_d >= '2007-01-02 00:00:00.000000'
        and o_entry_d < '2025-01-02 00:00:00.000000'
        and exists (select *
                from {snapshot_type('ORDER_LINE', sts)}
                where o_id = ol_o_id
                and o_w_id = ol_w_id
                and o_d_id = ol_d_id
                and ol_delivery_d >= o_entry_d)
    group by o_ol_cnt
    order by o_ol_cnt;
    """,

    # 5
    lambda sts=0: f"""
    select       n_name,
        sum(ol_amount) as revenue
    from         {snapshot_type('CUSTOMER', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_type('ORDER_LINE', sts)}, {snapshot_type('STOCK', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_native('NATION', sts)}, {snapshot_native('REGION', sts)}
    where        c_id = o_c_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and ol_o_id = o_id
        and ol_w_id = o_w_id
        and ol_d_id=o_d_id
        and ol_w_id = s_w_id
        and ol_i_id = s_i_id
        and mod((s_w_id * s_i_id),10000) = su_suppkey
        and ascii(substr(c_state,1,1))  = su_nationkey
        and su_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'Europe'
        and o_entry_d >= '2007-01-02 00:00:00.000000'
    group by n_name
    order by revenue desc
    """,

    # 6
    lambda sts=0: f"""
    select sum(ol_amount) as revenue
    from {snapshot_type('ORDER_LINE', sts)}
    where ol_delivery_d >= '1999-01-01 00:00:00.000000'
        and ol_delivery_d < '2025-01-01 00:00:00.000000'
        and ol_quantity between 1 and 100000;
    """,

    # 7
    lambda sts=0: f"""
    select       su_nationkey as supp_nation,
        substr(c_state,1,1) as cust_nation,
        extract(year from o_entry_d) as l_year,
        sum(ol_amount) as revenue
        from     {snapshot_native('SUPPLIER', sts)}, {snapshot_type('STOCK', sts)}, {snapshot_type('ORDER_LINE', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_type('CUSTOMER', sts)}, {snapshot_native('NATION', sts)}1, {snapshot_native('NATION', sts)}2
    where        ol_supply_w_id = s_w_id
        and ol_i_id = s_i_id
        and mod((s_w_id * s_i_id), 10000) = su_suppkey
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and c_id = o_c_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and su_nationkey = snap_NATION1.n_nationkey
        and ascii(substr(c_state,1,1)) - 32 = snap_NATION2.n_nationkey
        and (
            (snap_NATION1.n_name = 'Germany' and snap_NATION2.n_name = 'Cambodia')
            or
            (snap_NATION1.n_name = 'Cambodia' and snap_NATION2.n_name = 'Germany')
            )
        and ol_delivery_d between '2007-01-02 00:00:00.000000' and '2025-01-02 00:00:00.000000'
    group by su_nationkey, substr(c_state,1,1), extract(year from o_entry_d)
    order by su_nationkey, cust_nation, l_year
    """,

    # 8
    lambda sts=0: f"""
    select       extract(year from o_entry_d) as l_year,
        sum(case when snap_NATION2.n_name = 'Germany' then ol_amount else 0 end) / sum(ol_amount) as mkt_share
    from         {snapshot_native('ITEM', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_type('STOCK', sts)}, {snapshot_type('ORDER_LINE', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_type('CUSTOMER', sts)}, {snapshot_native('NATION', sts)}1, {snapshot_native('NATION', sts)}2, {snapshot_native('REGION', sts)}
    where        i_id = s_i_id
        and ol_i_id = s_i_id
        and ol_supply_w_id = s_w_id
        and mod((s_w_id * s_i_id),10000) = su_suppkey
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and c_id = o_c_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and snap_NATION1.n_nationkey = ascii(substr(c_state,1,1)) 
        and snap_NATION1.n_regionkey = r_regionkey
        and ol_i_id < 1000
        and r_name = 'Europe'
        and su_nationkey = snap_NATION2.n_nationkey
        and o_entry_d between '2007-01-02 00:00:00.000000' and '2025-01-02 00:00:00.000000'
        and i_data like '%b'
        and i_id = ol_i_id
    group by extract(year from o_entry_d)
    order by l_year
    """,

    # 9
    # ("and i_id = s_i_id" is necessary for tidb, otherwise it chooses a cartesian join)
    lambda sts=0: f"""
    select       n_name, extract(year from o_entry_d) as l_year, sum(ol_amount) as sum_profit
    from         {snapshot_native('ITEM', sts)}, {snapshot_type('STOCK', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_type('ORDER_LINE', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_native('NATION', sts)}
    where        ol_i_id = s_i_id
        and i_id = s_i_id
        and ol_supply_w_id = s_w_id
        and mod((s_w_id * s_i_id), 10000) = su_suppkey
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and ol_i_id = i_id
        and su_nationkey = n_nationkey
        and i_data like '%bb'
    group by n_name, extract(year from o_entry_d)
    order by n_name, l_year desc
    """,

    # 10
    lambda sts=0: f"""
    select       c_id, c_last, sum(ol_amount) as revenue, c_city, c_phone, n_name
    from         {snapshot_type('CUSTOMER', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_type('ORDER_LINE', sts)}, {snapshot_native('NATION', sts)}
    where        c_id = o_c_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and o_entry_d >= '2007-01-02 00:00:00.000000'
        and o_entry_d <= ol_delivery_d
        and n_nationkey = ascii(substr(c_state,1,1)) 
    group by c_id, c_last, c_city, c_phone, n_name
    order by revenue desc
    """,

    # 11
    lambda sts=0: f"""
    select       s_i_id, sum(s_order_cnt) as ordercount
    from         {snapshot_type('STOCK', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_native('NATION', sts)}
    where        mod((s_w_id * s_i_id),10000) = su_suppkey
        and su_nationkey = n_nationkey
        and n_name = 'Germany'
    group by s_i_id
    having   sum(s_order_cnt) >
            (select sum(s_order_cnt) * .005
            from {snapshot_type('STOCK', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_native('NATION', sts)}
            where mod((s_w_id * s_i_id),10000) = su_suppkey
            and su_nationkey = n_nationkey
            and n_name = 'Germany')
    order by ordercount desc
    """,

    # 12
    lambda sts=0: f"""
    select  o_ol_cnt,
        sum(case when o_carrier_id = 1 or o_carrier_id = 2 then 1 else 0 end) as high_line_count,
        sum(case when o_carrier_id <> 1 and o_carrier_id <> 2 then 1 else 0 end) as low_line_count
    from  {snapshot_type('ORDERS', sts)}, {snapshot_type('ORDER_LINE', sts)}
    where  ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
        and o_entry_d <= ol_delivery_d
        and ol_delivery_d < '2025-01-01 00:00:00.000000'
    group by o_ol_cnt
    order by o_ol_cnt;
    """,

    # 13
    lambda sts=0: f"""
    select  c_count, count(*) as custdist
    from  (select c_id as c_id, count(o_id) as c_count
        from {snapshot_type('CUSTOMER', sts)} left outer join {snapshot_type('ORDERS', sts)} on (
            c_w_id = o_w_id
            and c_d_id = o_d_id
            and c_id = o_c_id
            and o_carrier_id > 8)
        group by c_id) as c_ORDERS
    group by c_count
    order by custdist desc, c_count desc;
    """,

    # 14
    lambda sts=0: f"""
    select 100.00 * sum(case when i_data like 'PR%' then ol_amount else 0 end) / (1+sum(ol_amount)) as promo_revenue
    from {snapshot_type('ORDER_LINE', sts)}, {snapshot_native('ITEM', sts)}
    where ol_i_id = i_id and ol_delivery_d >= '2007-01-02 00:00:00.000000'
        and ol_delivery_d < '2025-01-02 00:00:00.000000'
    """,

    # 15
    lambda sts=0: f"""
    with         revenue (supplier_no, total_revenue) as (
        select  mod((s_w_id * s_i_id),10000) as supplier_no,
            sum(ol_amount) as total_revenue
        from    {snapshot_type('ORDER_LINE', sts)}, {snapshot_type('STOCK', sts)}
            where ol_i_id = s_i_id and ol_supply_w_id = s_w_id
            and ol_delivery_d >= '2007-01-02 00:00:00.000000'
        group by mod((s_w_id * s_i_id),10000))
    select       su_suppkey, su_name, su_address, su_phone, total_revenue
    from         {snapshot_native('SUPPLIER', sts)}, revenue
    where        su_suppkey = supplier_no
        and total_revenue = (select max(total_revenue) from revenue)
    order by su_suppkey
    """,

    # 16
    lambda sts=0: f"""
    select       i_name,
        substr(i_data, 1, 3) as brand,
        i_price,
        count(distinct (mod((s_w_id * s_i_id),10000))) as supplier_cnt
    from         {snapshot_type('STOCK', sts)}, {snapshot_native('ITEM', sts)}
    where        i_id = s_i_id
        and i_data not like 'zz%'
        and (mod((s_w_id * s_i_id),10000) not in
            (select su_suppkey
            from {snapshot_native('SUPPLIER', sts)}
            where su_comment like '%bad%'))
    group by i_name, substr(i_data, 1, 3), i_price
    order by supplier_cnt desc
    """,

    # 17
    lambda sts=0: f"""
    select sum(ol_amount) / 2.0 as avg_yearly
    from {snapshot_type('ORDER_LINE', sts)}, (select   i_id, avg(ol_quantity) as a
                from     {snapshot_native('ITEM', sts)}, {snapshot_type('ORDER_LINE', sts)}
                where    i_data like '%b'
                    and ol_i_id = i_id
                group by i_id) t
    where ol_i_id = t.i_id
        and ol_quantity <= t.a;
    """,

    # 18
    lambda sts=0: f"""
    select  c_last, c_id o_id, o_entry_d, o_ol_cnt, sum(ol_amount)
    from  {snapshot_type('CUSTOMER', sts)}, {snapshot_type('ORDERS', sts)}, {snapshot_type('ORDER_LINE', sts)}
    where  c_id = o_c_id
        and c_w_id = o_w_id
        and c_d_id = o_d_id
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_o_id = o_id
    group by o_id, o_w_id, o_d_id, c_id, c_last, o_entry_d, o_ol_cnt
    having  sum(ol_amount) > 200
    order by sum(ol_amount) desc, o_entry_d;
    """,

    # 19
    lambda sts=0: f"""
    select sum(ol_amount) as revenue
    from {snapshot_type('ORDER_LINE', sts)}, {snapshot_native('ITEM', sts)}
    where ol_i_id = i_id and (
        (
            i_data like '%a'
            and ol_quantity >= 1
            and ol_quantity <= 10
            and i_price between 1 and 400000
            and ol_w_id in (1,2,3)
        ) or (
        i_data like '%b'
        and ol_quantity >= 1
        and ol_quantity <= 10
        and i_price between 1 and 400000
        and ol_w_id in (1,2,4)
        ) or (
        i_data like '%c'
        and ol_quantity >= 1
        and ol_quantity <= 10
        and i_price between 1 and 400000
        and ol_w_id in (1,5,3)
        ));
    """,

    # 20
    lambda sts=0: f"""
    select       su_name, su_address
    from         {snapshot_native('SUPPLIER', sts)}, {snapshot_native('NATION', sts)}
    where        su_suppkey in
            (select  mod(s_i_id * s_w_id, 10000)
            from     {snapshot_type('STOCK', sts)}, {snapshot_type('ORDER_LINE', sts)}
            where    s_i_id in
                    (select i_id
                    from {snapshot_native('ITEM', sts)}
                    where i_data like 'co%')
                and ol_i_id=s_i_id
                and ol_delivery_d > '2010-05-23 12:00:00'
            group by s_i_id, s_w_id, s_quantity
            having   2*s_quantity > sum(ol_quantity))
        and su_nationkey = n_nationkey
        and n_name = 'Germany'
    order by su_name
    """,

    # 21
    lambda sts=0: f"""
    select       su_name, count(*) as numwait
    from  (
        select * from {snapshot_native('NATION', sts)}, {snapshot_native('SUPPLIER', sts)}, {snapshot_type('STOCK', sts)}
        where su_nationkey = n_nationkey
            and mod((s_w_id * s_i_id),10000) = su_suppkey
            and n_name = 'Germany'
    )  t_, {snapshot_type('ORDER_LINE', sts)}1, {snapshot_type('ORDERS', sts)}
    where        ol_o_id = o_id
        and ol_w_id = o_w_id
        and ol_d_id = o_d_id
        and ol_w_id = s_w_id
        and ol_i_id = s_i_id
        and ol_delivery_d > o_entry_d
        and not exists (select *
                from    {snapshot_type('ORDER_LINE', sts)}2
                where  snap_ORDER_LINE2.ol_o_id = snap_ORDER_LINE1.ol_o_id
                    and snap_ORDER_LINE2.ol_w_id = snap_ORDER_LINE1.ol_w_id
                    and snap_ORDER_LINE2.ol_d_id = snap_ORDER_LINE1.ol_d_id
                    and snap_ORDER_LINE2.ol_delivery_d > snap_ORDER_LINE1.ol_delivery_d)
    group by su_name
    order by numwait desc, su_name
    """,

    # 22
    lambda sts=0: f"""
    select  substr(c_state,1,1) as country,
        count(*) as numcust,
        sum(c_balance) as totacctbal
    from  {snapshot_type('CUSTOMER', sts)}
    where  substr(c_phone,1,1) in ('1','2','3','4','5','6','7')
        and c_balance > (select avg(c_BALANCE)
                from   {snapshot_type('CUSTOMER', sts)}
                where  c_balance > 0.00
                    and substr(c_phone,1,1) in ('1','2','3','4','5','6','7'))
        and not exists (select *
                from {snapshot_type('ORDERS', sts)}
                where o_c_id = c_id
                        and o_w_id = c_w_id
                        and o_d_id = c_d_id)
    group by substr(c_state,1,1)
    order by substr(c_state,1,1);
    """,
]


snapshot_types = {
    'snapshot_native': snapshot_native,
    'snapshot_cache_storage_union': snapshot_cache_storage_union,
    'snapshot_cache_storage_fulljoin': snapshot_cache_storage_fulljoin
}


def begin_tique_tx(cursor):
    cursor.execute("select txid_sequence(false), sts_sequence('get', null)")
    txid, sts = cursor.fetchone()
    cursor.execute(f"insert into log_began values ({txid}, {sts})")
    return txid, sts


def commit_tique_tx(cursor, txid, sts):
    cursor.execute(f"insert into log_aborted values ({txid}, {sts})") 


def run_query(cursor, query, tique, runs=1):
    begin = time.time()
    for _ in range(runs):
        if tique:
            txid, sts = begin_tique_tx(cursor)
            cursor.execute(query(sts))
            cursor.fetchall()
            commit_tique_tx(cursor, txid, sts)
        else:
            # tidb with one worker thread returns an error while executing query 16
            try:
                cursor.execute(query())
                cursor.fetchall()
            except Exception:
                traceback.print_exc()
    return time.time() - begin


def create_extra_indexes(conn_str):
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    indexes = [
        'create index stock_s_i_id_idx on STOCK (s_i_id)',
        'create index order_line_ol_i_id_idx on ORDER_LINE (ol_i_id)',
        'create index stock_mod_idx on STOCK (mod((s_w_id * s_i_id), 10000))',
        'create index orders_o_carrier_d_idx on ORDERS (o_carrier_id)',
        'create index customer_c_balance_idx on CUSTOMER (c_balance)',
        'create index stock_s_i_id_idx on STOCK_STORAGE (s_i_id)',
        'create index order_line_ol_i_id_idx on ORDER_LINE_STORAGE (ol_i_id)',
        'create index stock_mod_idx on STOCK_STORAGE (mod((s_w_id * s_i_id), 10000))',
        'create index orders_o_carrier_d_idx on ORDERS_STORAGE (o_carrier_id)',
        'create index customer_c_balance_idx on CUSTOMER_STORAGE (c_balance)'
    ]

    for index in indexes:
        try:
            print(index)
            cursor.execute(index)
        except Exception as e:
            print(e)
            pass


def runner(args):
    worker_id, conn_str, engine, queries_queue, duration, repeats, completed_list = args
    conn = pyodbc.connect(conn_str)
    conn.autocommit = True
    cursor = conn.cursor()
    count = 0
    executed_queries = []

    if engine == 'postgres':
        # increase the seq_page_cost in order to use indexes in query 17 
        cursor.execute('set seq_page_cost = 4;')
        # increases number of workers for large, parallel queries
        cursor.execute('set max_parallel_workers = 32;')
        cursor.execute('set max_parallel_workers_per_gather = 1;')
        cursor.execute('set parallel_setup_cost = 0')
        cursor.execute('set parallel_tuple_cost = 0')
    elif engine == 'singlestore':
        cursor.execute('set session query_parallelism_per_leaf_core = 0.3333333333333333')
    elif engine == 'tidb':
        cursor.execute('set session tidb_distsql_scan_concurrency = 4')
        cursor.execute('set session tidb_executor_concurrency = 4')
        cursor.execute('set session tidb_max_tiflash_threads = 4')
        cursor.execute('set session tidb_mem_quota_query = -1')
        cursor.execute("set session tidb_isolation_read_engines = 'tiflash,tidb'")
        cursor.execute("set global tidb_server_memory_limit = 0")

    queries_native = queries_olap(snapshot_native)
    queries_custom_union = queries_olap(snapshot_cache_storage_union)

    estimated_end = time.time() + duration
    while duration == -1 or time.time() < estimated_end:
        query_idx = queries_queue.get()

        # execute query
        if query_idx is not None:
            if engine.startswith('tique'):
                query, snapshot_name = queries_custom_union[query_idx], 'snapshot_cache_storage_union'
            else:
                query, snapshot_name = queries_native[query_idx], 'snapshot_native'
            begin_query = time.time()
            rt = run_query(cursor, query, tique=snapshot_name!='snapshot_native', runs=repeats)
            end_query = time.time()

            # query completed in the valid time
            if duration == -1 or end_query < estimated_end:
                count += 1
            # query partially completed in the valid time
            else:
                count += (estimated_end - begin_query) / (end_query - begin_query)

            executed_queries.append(query_idx)
            print(f'{engine},query-{query_idx+1},{rt}')

        # regular execution has finished
        else:
            end_time = time.time()
            # mark as completed
            completed_list[worker_id] = True

            if len(executed_queries) > 0:
                executed_query_id = 0
                # keep executing previously completed queries while waiting for the other workers
                while not all(completed_list):
                    if engine.startswith('tique'):
                        query = queries_custom_union[executed_queries[executed_query_id]]
                        run_query(cursor, query, tique=True, runs=1)
                    else:
                        query = queries_native[executed_queries[executed_query_id]]
                        run_query(cursor, query, tique=False, runs=1)
                    executed_query_id = (executed_query_id + 1) % len(executed_queries)
     
            return count, end_time

    return count, time.time()


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('engine', type=str, help='Engine (postgres|monetdb*|tique*|singlestore|tidb)')
    parser.add_argument('-c', '--clients', type=int, help='Number of clients', action='store', required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-t', '--time', type=int, help='Duration in seconds', action='store')
    group.add_argument('-r', '--rounds', type=int, help='Number of rounds to execute', action='store')
    parser.add_argument('-q', '--repeats', type=int, help='Number of times a query is repeated', default=1, action='store', required=False)
    parser.add_argument('--no-execute', help='With this flag the necessary indexes are created but the benchmark is not executed', default=False, action='store_true', required=False)
    parser.add_argument('--keep-executing', help='Keep the workers executing until after all initial planned queries have been completed', default=False, action='store_true', required=False)
    args = parser.parse_args()

    engine = args.engine
    clients = args.clients
    repeats = args.repeats
    duration = args.time
    number_of_runs = args.rounds
    no_execute = args.no_execute
    keep_executing = args.keep_executing

    if engine.startswith('monetdb') or engine.startswith('tique'):
        conn_str = "DRIVER=/usr/lib/x86_64-linux-gnu/libMonetODBC.so;HOST=127.0.0.1;PORT=50000;DATABASE=tpcc;UID=monetdb;PWD=monetdb"
    elif engine == 'postgres':
        conn_str = "DRIVER=PostgreSQL Unicode;HOST=127.0.0.1;PORT=5432;DATABASE=tpcc;UID=postgres;PWD=postgres"
    elif engine == 'singlestore':
        conn_str = 'DRIVER=/usr/lib/x86_64-linux-gnu/libssodbcw.so;SERVER=127.0.0.1;PORT=3306;DATABASE=tpcc;UID=root;PWD=root'
    elif engine == 'tidb':
        conn_str = 'DRIVER=MySQL ODBC 8.0 Unicode Driver;SERVER=127.0.0.1;PORT=4000;DATABASE=tpcc;UID=root'
    else:
        exit('Engine not available (available engines: monetdb, postgres, tique, singlestore, tidb)')

    create_extra_indexes(conn_str)
    
    if no_execute:
        exit(0)
        
    if duration is None:
        duration = -1
    else:
        number_of_runs = 300

    manager = Manager()
    # queue of jobs
    queries_queue = manager.Queue()
    n_queries = len(queries_olap(snapshot_native))
    for i in [j % n_queries for j in range(n_queries * number_of_runs)]:
        queries_queue.put(i)
    # list where each index states if the respective worker finished the end of its execution
    # (to be used when we want workers to keep executing while waiting for the others)
    completed_list = manager.list()
    for _ in range(clients):
        completed_list.append(not keep_executing)
    # stop flag
    for _ in range(clients):
        queries_queue.put(None)

    print('driver,query,rt')
    
    pool = Pool(int(sys.argv[2]))
    begin = time.time()
    partial_results = pool.map(runner, [(i, conn_str, engine, queries_queue, duration, repeats, completed_list) for i in range(clients)])
    runtime = round(max([x[1] for x in partial_results]) - begin, 3)
    total_count = round(sum([x[0] for x in partial_results]), 3)
    total_txs = round(total_count / (duration if duration != -1 else runtime), 3)

    print()
    print(f'Throughput: {total_txs} tx/s')
    print(f'Total duration: {runtime}s')
    print(f'Count: {total_count} tx')
    print(f'csv::{total_txs},{total_count},{runtime}')


if __name__ == '__main__':
    main()
