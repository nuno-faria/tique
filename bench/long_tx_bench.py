import hashlib
from statistics import mean
import pyodbc
import argparse
from multiprocessing import Pool
import time
import random
import secrets


# snapshot code
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


STATEMENTS = {
    '_00_get_txid_sts': "select txid_sequence(0), sts_sequence('get', null)",
    'get_txid_sts': lambda: "exec 0()",
    '_01_get_sts': "select sts_sequence('get', null)",
    'get_sts': lambda: "exec 1()",
    '_02_insert_log_began': "insert into log_began values ($, $, $)",
    'insert_log_began': lambda txid, sts, long_running: f"exec 2({txid}, {sts}, {long_running})",
    '_03_insert_log_aborted': "insert into log_aborted values ($)",
    'insert_log_aborted': lambda txid: f"exec 3({txid})",
    '_04_get_prev_ctn': "select ctn from log_committing where txid = $",
    'get_prev_ctn': lambda txid: f"exec 4({txid})",
    '_05_insert_log_committing': "insert into log_committing values ($, (select ctn_sequence('acquire')))",
    'insert_log_committing': lambda txid: f"exec 5({txid})",
    '_06_release_ctn': "select ctn_sequence('release')",
    'release_ctn': lambda: f"exec 6()",
    # the commented code below is used to give extra priority to long-running transactions;
    # with this code, regular transactions abort even if the long-running are still running;
    # although in theory this reduces abort probability for long-running transactions, in practice
    # it results in the same probability since in this workload long-running transactions
    # end up aborting the first time due to a write already committed by another regular transaction;
    # thus, since in this workload this increases abort probability for regular transactions
    # and has the same behavior for long-running ones, it is best to not use it.
    #'_07_certify': f"""
    #    SELECT count(*) > 0
    #    FROM Write_Sets WS
    #    WHERE pk IN (SELECT pk FROM Write_Sets WHERE txid = $)
    #        AND txid <> $ -- do not match my write set
    #        AND (txid IN (SELECT txid FROM Log_committed WHERE cts > $) -- transaction committed after mine started
    #            OR (txid NOT IN (SELECT txid FROM Log_committed) -- transaction not committed,
    #                AND txid NOT IN (SELECT txid FROM Log_aborted) -- not aborted
    #                AND ((
    #                    -- the transaction is long-running and mine is not
    #                    txid IN (SELECT txid FROM Log_began WHERE long_running)
    #                    AND NOT (SELECT long_running FROM Log_began WHERE txid = $)
    #                ) OR (
    #                 txid IN (SELECT txid FROM Log_committing WHERE ctn < $)
    #                ))
    #            )
    #        )
    #""",
    '_07_certify': """
        SELECT count(*) > 0
        FROM Write_Sets WS
        WHERE pk IN (SELECT pk FROM Write_Sets WHERE txid = $)
            AND txid <> $ -- do not match my write set
            AND (txid IN (SELECT txid FROM Log_committed WHERE cts > $) -- transaction committed after mine started
                OR (txid NOT IN (SELECT txid FROM Log_committed) -- transaction not committed,
                    AND txid NOT IN (SELECT txid FROM Log_aborted) -- not aborted
                    AND txid IN (SELECT txid FROM Log_committing WHERE ctn < (select ctn from log_committing where txid = $))
                )
            )
    """,
    "certify": lambda txid, sts: f"exec 7({txid}, {txid}, {sts}, {txid})",
    "_08_insert_write_set": "insert into write_sets values ($, $)",
    "insert_write_set": lambda k, v: f"exec 8({k}, {v})",
    '_09_insert_log_committed': "insert into log_committed values ($, (select cts_sequence(0)))",
    'insert_log_committed': lambda txid: f"exec 9({txid})",
    '_10_increment_sts': "select sts_sequence('waitAndAdvance', (select cts from log_committed where txid = $))",
    'increment_sts': lambda txid: f"exec 10({txid})",
    '_11_insert_table': "insert into data_cache values ($, $, $, $)",
    'insert_table': lambda k, v, deleted, txid: f"exec 11('{k}', '{v}', {deleted}, {txid})",
    '_12_read': f"select v from {snapshot('$')} where k = $",
    'read': lambda sts, k: f"exec 12({sts}, '{k}')"
}


# returns a connection to the database
def connect(connStr):
    conn = pyodbc.connect(connStr)
    conn.autocommit = True
    return conn


# generates a random database key
def randomKey(maxSize):
    return f'k{random.randint(0, maxSize)}'


# generates a random database value
def randomValue():
    return secrets.token_urlsafe(16)


# creates the schema and populates it
def populate(engine, connStr, size):
    conn = connect(connStr)
    cursor = conn.cursor()

    if engine == 'tique':
        cursor.execute('''
            create table if not exists data_storage (k varchar(255), v varchar(1024));
            create table if not exists data_cache (k varchar(255), v varchar(1024), deleted boolean, txid bigint);
            create unlogged table if not exists log_began (txid bigint, sts bigint, long_running boolean);
            create unlogged table if not exists log_committing (txid bigint, ctn bigint);
            create unlogged table if not exists log_aborted (txid bigint);
            create table if not exists log_committed (txid bigint, cts bigint);
            create unlogged table if not exists write_sets (txid bigint, pk bigint);
            delete from data_storage;
            delete from data_cache;
            delete from log_began;
            delete from log_committing;
            delete from log_committed;
            delete from log_aborted;
            delete from write_sets;
        ''')

        conn.autocommit = False
        for i in range(size):
            cursor.execute(f"insert into data_storage values ('k{i}', {i})")
        cursor.execute('commit')
        conn.autocommit = True

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
        ''')

        cursor.execute(f"select sts_sequence('init', 1)")
        cursor.execute(f"select txid_sequence(1)")
        cursor.execute(f"select ctn_sequence('reset')")
        cursor.execute(f"select cts_sequence(2)")
    else:
        cursor.execute('create table if not exists data (k varchar(255) primary key, v varchar(1024));')
        cursor.execute('delete from data;')
        
        conn.autocommit = False
        for i in range(size):
            cursor.execute(f"insert into data values ('k{i}', {i})")
        cursor.execute('commit')

    conn.close()


# begins a transaction
def begin(engine, cursor, longRunning=False):
    if engine == 'tique':
        cursor.execute(STATEMENTS['get_txid_sts']())
        txid, sts = cursor.fetchone()
        cursor.execute(STATEMENTS['insert_log_began'](txid, sts, longRunning))
        return txid, sts
    else:
        return None, None


# performs a simple read
def read(engine, cursor, sts, key):
    if engine == 'tique':
        cursor.execute(STATEMENTS['read'](sts, key))
    else:
        cursor.execute(f"select v from data where k = '{key}'")

    return cursor.fetchone()


# converts an object to string and hashes that string to fit in a bigint (tique)
def hashObject(s):
    return int(hashlib.sha1(str(s).encode("utf-8")).hexdigest(), 16) % 18446744073709551614 - 9223372036854775807


# adds an entry to the write sets table
def write(engine, cursor, txid, key, value):
    if engine == 'tique':
        cursor.execute(STATEMENTS['insert_write_set'](txid, hashObject(key)))
    else:
        cursor.execute(f"update data set v = '{value}' where k = '{key}'")


# commits (or aborts) a transaction
def commit(engine, cursor, txid, sts, writes, long_running = False):
    if engine != 'tique':
        cursor.execute('commit')
        return True

    if len(writes) == 0:
        cursor.execute(STATEMENTS['insert_log_aborted'](txid))
        return True

    # if long-running, reuse the previous ctn (if one is available)
    ctn = None
    if long_running:
        cursor.execute(STATEMENTS['get_prev_ctn'](txid))
        r = cursor.fetchone()
        if r:
            ctn = r[0]

    # acquire ctn and update the log
    if ctn is None:
        cursor.execute(STATEMENTS['insert_log_committing'](txid) + ";" + STATEMENTS['release_ctn']())

    # check conflicts
    cursor.execute(STATEMENTS['certify'](txid, sts))
    has_conflicts = cursor.fetchone()[0]

    # abort
    if has_conflicts:
        if not long_running:
            cursor.execute(STATEMENTS['insert_log_aborted'](txid))
        return False
    # commit
    else:
        # add data
        # transaction block just to batch writes
        cursor.execute('begin transaction')
        for key, value in writes.items():
            cursor.execute(STATEMENTS['insert_table'](key, value, False, txid))
        cursor.execute(STATEMENTS['insert_log_committed'](txid))
        cursor.execute('commit')

        # wait and advance sts
        cursor.execute(STATEMENTS['increment_sts'](txid))

        return True


# updates a (long-running) transaction's sts to the most recent one
def restartTx(cursor, txid):
    cursor.execute(STATEMENTS['get_sts']())
    sts = cursor.fetchone()[0]
    cursor.execute(STATEMENTS['insert_log_began'](txid, sts, True))
    return sts


# contains the transactional logic - simple reads and writes
def runTx(engine, cursor, txid, sts, ops):
    writeSet = {}

    for op in ops:
        action, key = op

        if action == 'r':
            read(engine, cursor, sts, key)
        else:
            value = randomValue()
            writeSet[key] = value
            write(engine, cursor, txid, key, value)

    return writeSet


# executes a regular transaction
def regularTx(engine, cursor, ops):
    aborts = 0
    result = False
    
    while not result:
        txid, sts = begin(engine, cursor)
        try:
            writeSets = runTx(engine, cursor, txid, sts, ops)
            result = commit(engine, cursor, txid, sts, writeSets)
            aborts += 1 if not result else 0
        except:
            cursor.execute('rollback')
            aborts += 1

    return (1, aborts)


# executes a long-running transaction, i.e., retried until it completes with success
def longTx(engine, cursor, ops, handleLongRunning):
    txid = None
    aborts = 0
    result = False

    while not result:
        if txid is None or engine != 'tique' or not handleLongRunning:
            txid, sts = begin(engine, cursor, longRunning=handleLongRunning)
        # retry
        else:
            sts = restartTx(cursor, txid)

        try:
            writeSets = runTx(engine, cursor, txid, sts, ops)
            result = commit(engine, cursor, txid, sts, writeSets, long_running=handleLongRunning)
        except:
            cursor.execute('rollback')
            result = False

        if not result:
            aborts += 1

    return 1, aborts


# client logic; returns the number of commits, number of aborts, and the average response time
def client(args):
    totalCommits = 0
    totalAborts = 0
    totalRt = 0
    conn = connect(args['connStr'])
    cursor = conn.cursor()
    if args['setupSql'] is not None:
        cursor.execute(args['setupSql'])
    if args['engine'] != 'tique':
        conn.autocommit = False
    else:
        for name, statement in sorted(STATEMENTS.items()):
            if name.startswith('_'):
                cursor.execute(f'prepare {statement}')
    begin = time.time()
    
    while time.time() - begin < args['time']:
        nReads = args['nOps'] // 2
        nWrites = args['nOps'] // 2
        reads = [(str('r'), randomKey(args['size'])) for _ in range(nReads)]
        writes = [(str('w'), randomKey(args['size'])) for _ in range(nWrites)]
        ops = [*reads, *writes]
        # shuffle ops
        random.shuffle(ops)
        # sort by key to avoid deadlocks
        ops = sorted(ops, key=lambda x: x[1])

        b = time.time()
        if args['type'] == 'regular':
            commits, aborts = regularTx(args['engine'], cursor, ops)
        else:
            commits, aborts = longTx(args['engine'], cursor, ops, args['handleLongRunning'])
        rt = time.time() - b

        totalCommits += commits
        totalAborts += aborts

        if commits > 0:
            totalRt += rt

    return (totalCommits, totalAborts, totalRt / totalCommits)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('engine', type=str, help='Engine (postgres-rr|postgres-rc|monetdb|tique|' +
                        'singlestore|tidb-pessimistic|tidb-optimistic|tidb-pessimistic-long-only)')
    parser.add_argument('-c', '--clients', type=int, nargs='+', help='Number of clients', action='store', required=True)
    parser.add_argument('-t', '--time', type=int, help='Duration in seconds', action='store', required=True)
    parser.add_argument('-l', '--handle-long-running', default=False, action='store_true',
                        help='Use this flag to handle long-running transactions as such (for TiQuE).')
    parser.add_argument('-s', '--size', type=int, help='Number of rows', action='store', default=10000)
    parser.add_argument('--regular-ops', type=int, help='Number of operations for regular transactions',
                        action='store', default=10)
    parser.add_argument('--long-ops', type=int, help='Number of operations for long transactions',
                        action='store', default=1000)
    args = parser.parse_args()

    setupSql = None
    setupSqlLong = None
    if args.engine.startswith('postgres'):
        connStr = 'DRIVER=PostgreSQL Unicode;HOST=127.0.0.1;PORT=5432;DATABASE=tpcc;UID=postgres;PWD=postgres'
        isolation = 'repeatable read' if args.engine == 'postgres-rr' else 'read committed'
        setupSql = f'SET SESSION CHARACTERISTICS AS TRANSACTION isolation level {isolation}'
    elif args.engine == 'monetdb' or args.engine == 'tique':
        connStr = 'DRIVER=/usr/lib/x86_64-linux-gnu/libMonetODBC.so;HOST=127.0.0.1;PORT=50000;DATABASE=tpcc;UID=monetdb;PWD=monetdb'
        setupSql = "set optimizer = 'minimal_fast'"
    elif args.engine == 'singlestore':
        connStr = 'DRIVER=/usr/lib/x86_64-linux-gnu/libssodbcw.so;SERVER=127.0.0.1;PORT=3306;DATABASE=tpcc;UID=root;PWD=root'
    elif args.engine.startswith('tidb'):
        connStr = 'DRIVER=MySQL ODBC 8.0 Unicode Driver;SERVER=127.0.0.1;PORT=4000;DATABASE=tpcc;UID=root'
        execution = 'pessimistic' if args.engine == 'tidb-pessimistic' else 'optimistic'
        setupSql = f"SET SESSION tidb_txn_mode = '{execution}'"
        executionLong = 'pessimistic' if 'pessimistic' in args.engine else 'optimistic'
        setupSqlLong = f"SET SESSION tidb_txn_mode = '{executionLong}'"
    else:
        exit('Invalid engine. Must be one of postgres-rr,postgres-rc,monetdb,tique,singlestore,' +
             'tidb-pessimistic,tidb-optimistic')

    regularArgs = {
        'type': 'regular',
        'time': args.time,
        'size': args.size,
        'nOps': args.regular_ops,
        'engine': args.engine,
        'connStr': connStr,
        'setupSql': setupSql
    }

    longArgs = {
        'type': 'long',
        'time': args.time,
        'size': args.size,
        'nOps': args.long_ops,
        'handleLongRunning': args.handle_long_running,
        'engine': args.engine,
        'connStr': connStr,
        'setupSql': setupSqlLong if setupSqlLong is not None else setupSql
    }

    print('engine,clients,size,regularOps,longOps,handleLongRunning,regularCommits,regularTxs,' \
          + 'regularAr,regularRt,longCommits,longTxs,longAr,longRt,txs,ar')

    for clients in args.clients:
        populate(args.engine, connStr, args.size)

        pool = Pool(clients)
        resultsRegular = pool.map_async(client, [regularArgs for _ in range(clients - 1)])
        resultsLong = pool.map_async(client, [longArgs])
        resultsRegular = resultsRegular.get()
        resultsLong = resultsLong.get()

        regularCommits = sum([x[0] for x in resultsRegular]) if len(resultsRegular) > 0 else 0
        regularAborts = sum([x[1] for x in resultsRegular]) if len(resultsRegular) > 0 else 0
        rtRegular = round(mean([x[2] for x in resultsRegular]), 3) if len(resultsRegular) > 0 else 0
        longCommits = sum([x[0] for x in resultsLong])
        longAborts = sum([x[1] for x in resultsLong])
        rtLong = round(mean([x[2] for x in resultsLong]), 3)

        regularTxs = round(regularCommits / args.time, 2)
        longTxs = round(longCommits / args.time, 2)
        regularAr = round(regularAborts / (regularAborts + regularCommits), 2) if (regularAborts + regularCommits) else 0
        longAr = round(longAborts / (longAborts + longCommits), 2)
        txs = round((regularCommits + longCommits) / args.time, 2)
        ar = round((regularAborts + longAborts) / (regularCommits + regularAborts + longCommits + longAborts), 2)

        print(f'{args.engine},{clients},{args.size},{args.regular_ops},{args.long_ops},{args.handle_long_running},'
            + f'{regularCommits},{regularTxs},{regularAr},{rtRegular},{longCommits},{longTxs},{longAr},'
            + f'{rtLong},{txs},{ar}')

        pool.close()


if __name__ == '__main__':
    main()
