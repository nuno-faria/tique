from collections import defaultdict
import sys
from multiprocessing import Pool, Manager, Queue
import json
import time
import traceback
import pyodbc
import hashlib


PROCESSES = 8
OUT = sys.argv[2] if len(sys.argv) >= 3 else 'history.edn'
CONN_STR = 'DRIVER=/usr/lib/x86_64-linux-gnu/libMonetODBC.so;HOST=127.0.0.1;PORT=50000;DATABASE=tpcc;UID=monetdb;PWD=monetdb'


snapshot = lambda sts: f"""
(
    select k, v
    from (
        select *, rank() over (partition by k order by cts desc) as rank
        from (
            (SELECT k, v, false as deleted, 0 as cts
            FROM data_storage)
            UNION ALL
            (SELECT k, v, deleted, cts + 0 as cts
            FROM data_cache
            JOIN Log_committed ON Log_committed.txid = data_cache.txid)
        ) t1
        WHERE cts <= {sts}
    ) t2 where not deleted and rank = 1
) data_snap """

# another snapshot
#snapshot = lambda sts: f"""
#    ((SELECT k, v
#    FROM data_storage s
#    WHERE (k) NOT IN (
#        SELECT k
#        FROM data_cache c
#        JOIN Log_committed ON Log_committed.txid = c.txid
#        WHERE Log_committed.cts <= {sts}
#    ))
#    UNION ALL
#    (SELECT k, v
#    FROM (
#        SELECT k, v, deleted + 0 as deleted
#        FROM data_cache c
#        JOIN Log_committed ON Log_committed.txid = c.txid
#        WHERE (k, cts) IN (
#            SELECT k, max(cts)
#            FROM data_cache c
#            JOIN Log_committed ON Log_committed.txid = c.txid
#            WHERE cts <= {sts}
#            GROUP BY k
#        )
#    ) _t WHERE NOT deleted
#    )) data_snap """

def create_db():
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute('create table if not exists data_storage (k varchar(255), v varchar(1024))')
    cursor.execute('create table if not exists data_cache (k varchar(255), v varchar(1024), deleted boolean, txid bigint)')
    cursor.execute('create unlogged table if not exists log_began (txid bigint, sts bigint)')
    cursor.execute('create unlogged table if not exists log_committing (txid bigint, ctn bigint)')
    cursor.execute('create unlogged table if not exists log_aborted (txid bigint)')
    cursor.execute('create table if not exists log_committed (txid bigint, cts bigint)')
    cursor.execute('create unlogged table if not exists write_sets (txid bigint, pk bigint)')
    
    cursor.execute('delete from data_storage')
    cursor.execute('delete from data_cache')
    cursor.execute('delete from log_began')
    cursor.execute('delete from log_committing')
    cursor.execute('delete from log_committed')
    cursor.execute('delete from log_aborted')
    cursor.execute('delete from write_sets')

    cursor.execute('''
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
            delete
            from data_storage
            where (k) in (
                select k
                from (
                    select k, rank() over (partition by k order by cts desc) as rank
                    from data_cache
                    join log_committed on log_committed.txid = data_cache.txid
                    where cts <= stable_sts
                ) as t
                where rank = 1
            );

            -- move latest stable cache data to the respective storage
            insert into data_storage (k, v)
            select k, v
            from (
                select k, v, rank() over (partition by k order by cts desc) as rank, deleted
                from data_cache
                join log_committed on log_committed.txid = data_cache.txid
                where cts <= stable_sts
            ) as t 
            where rank = 1 and not deleted;

            -- delete the stable data from the cache tables
            -- delete obsolete versions first to prevent inconsistent reads 
            -- (if we remove the most recent version first, a transaction can read the previous version over the data written in the storage)
            delete
            from data_cache
            where (k, txid) in (
                select k, txid
                from (
                    select k, data_cache.txid, rank() over (partition by k order by cts desc) as rank
                    from data_cache
                    join log_committed on log_committed.txid = data_cache.txid
                    where cts <= stable_sts
                ) as t
                where rank > 1
            );

            -- can now delete the most recent versions safely
            delete
            from data_cache
            where (k, txid) in (
                select k, data_cache.txid
                from data_cache
                join log_committed on log_committed.txid = data_cache.txid
                where cts <= stable_sts
            );

            return stable_sts;
        end;
    ''')
    
    cursor.execute(f"select sts_sequence('init', 1)")
    cursor.execute(f"select txid_sequence(1)")
    cursor.execute(f"select ctn_sequence('reset')")
    cursor.execute(f"select cts_sequence(2)")
    cursor.execute(f'select flush()')

    conn.close()


# logs a transaction to OUT
def logger(log_queue: Queue, size: int):
    log = open(OUT, 'w')
    index = 1
    
    while True:
        txn = log_queue.get()
        if txn is None:
            size -= 1
            if size == 0:
                log.flush()
                log.close()
                return
            else:
                continue
        
        txn['index'] = index
        index += 1
        entry = json.dumps(txn) + '\n'
        entry = entry.replace('null', 'nil')
        entry = entry.replace('":', '')
        entry = entry.replace('",', ',')
        entry = entry.replace('"', ':')
        
        log.write(entry)


# begins a transaction
def begin(cursor):
    cursor.execute("select txid_sequence(0), sts_sequence('get', null)")
    txid, sts = cursor.fetchone()
    cursor.execute(f"insert into log_began values ({txid}, {sts})")
    return txid, sts


# converts an object to string and hashes that string to fit in a bigint
def hashObject(s):
    return int(hashlib.sha1(str(s).encode("utf-8")).hexdigest(), 16) % 18446744073709551614 - 9223372036854775807


# commits (or aborts) a transaction
def commit(cursor, txid, sts, writes):
    if len(writes) == 0:
        cursor.execute(f"insert into log_aborted values ({txid})")
        return True
    
    # add write set keys
    for key in writes.keys():
        cursor.execute(f"insert into write_sets values ({txid}, {hashObject(key)})")

    # acquire ctn and update log
    cursor.execute("select ctn_sequence('acquire')")
    ctn = cursor.fetchone()[0]
    cursor.execute(f"insert into log_committing values ({txid}, {ctn})")
    cursor.execute("select ctn_sequence('release')")
        
    # check conflicts
    cursor.execute(f"""
        SELECT count(*) > 0
        FROM Write_Sets WS
        WHERE pk IN (SELECT pk FROM Write_Sets WHERE txid = {txid})
            AND txid <> {txid} -- do not match my write set
            AND (txid IN (SELECT txid FROM Log_committed WHERE cts > {sts}) -- transaction committed after mine started
                OR (txid NOT IN (SELECT txid FROM Log_committed) -- transaction not committed,
                    AND txid IN (SELECT txid FROM Log_committing WHERE ctn < {ctn}) -- committing before me,
                    AND txid NOT IN (SELECT txid FROM Log_aborted))) -- and not aborted
    """)
    # CONFLICT VIOLATION: consider that all transactions commit (to check the anomalies returned by Elle)
    #cursor.execute("select 0")
    has_conflicts = cursor.fetchone()[0]
    
    # abort
    if has_conflicts:
        cursor.execute(f"insert into log_aborted values ({txid})")
        return False
    # commit
    else:
        # add data
        for key, writes in writes.items():
            writes_str = '.'.join([str(w) for w in writes])
            cursor.execute(f"insert into data_cache values ('{key}', '{writes_str}', false, {txid})")
        
        # acquire cts and update log
        cursor.execute(f"insert into log_committed values ({txid}, (select cts_sequence(0)))")

        # wait and advance sts
        cursor.execute(f"select sts_sequence('waitAndAdvance', (select cts from log_committed where txid = {txid}))")

        return True


# get a value - list of integers - given some key from the database 
def get_value(cursor, key, sts):
    cursor.execute(f'select v from {snapshot(sts)} where k = {key}')
    value = cursor.fetchone()
    if value is not None:
        return [int(x) for x in value[0].split('.')]
    else:
        return []


# executes a transaction
def execute(txn, cursor):
    try:
        out_values = []
        writes = defaultdict(list)

        txid, sts = begin(cursor)
        
        # flush cache data periodically
        if txid % 1000 == 0:
            cursor.execute('select flush()')

        for op, key, value in txn['value']:
            # SNAPSHOT VIOLATION: update the starting timestamp for every operation (to check the anomalies returned by Elle)
            #cursor.execute("select sts_sequence('get', null)")
            #sts = cursor.fetchone()[0]

            if op == 'r':
                # always read from the database to test the snapshot
                value_db = get_value(cursor, key, sts)
                # append local writes
                if key in writes:
                    value_db.extend(writes[key])
                if len(value_db) > 0:
                    out_values.append((op, key, value_db))
                else:
                    out_values.append((op, key, None))
            else:
                writes[key].append(value)
                out_values.append((op, key, value))

        txn['value'] = out_values

        # complete the writes with the previous version, since the benchmark appends values
        for key, values_written in writes.items():
            value_db = get_value(cursor, key, sts)
            value_db.extend(values_written)
            writes[key] = value_db

        if (commit(cursor, txid, sts, writes)):
            txn['type'] = 'ok'
            return txn
        else:
            txn['type'] = 'fail'
            return txn
    except:
        traceback.print_exc()


# executes multiples transactions and sends the result to the log
def runner(pid, queue: Queue, log_queue: Queue):
    conn = pyodbc.connect(CONN_STR)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("set optimizer = 'minimal_fast'")

    while True:
        in_txn = queue.get()

        # done
        if in_txn is None:
            log_queue.put(None)
            return
        
        # init req transaction data
        in_txn['process'] = pid
        in_txn['time'] = time.time_ns()
        log_queue.put(in_txn)

        # execute transaction
        out_txn = execute(in_txn, cursor)
        out_txn['time'] = time.time_ns()
        log_queue.put(out_txn)
        


def main():
    create_db()
    pool = Pool(PROCESSES + 1)
    m = Manager()
    queue = m.Queue()
    log_queue = m.Queue()

    r = pool.apply_async(logger, (log_queue, PROCESSES))
    
    for i in range(PROCESSES):
        pool.apply_async(runner, (i, queue, log_queue))

    with open(sys.argv[1]) as f:
        for line in f:
            queue.put(json.loads(line))

    for i in range(PROCESSES):
        queue.put(None)

    r.wait()


if __name__ == '__main__':
    main()
