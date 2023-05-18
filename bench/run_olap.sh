# Configs
DRIVER=tique # (tique | monetdb | monetdbsinglekey (recommended for native monetbd) | postgres | singlestore | tidb)
WAREHOUSES=512
SCALE=1
ROUNDS=1 # number of times to execute the benchmark (total queries = ROUNDS * 22)
RUNS_PER_QUERY=5
CLIENTS=(1)
MAX_WORKERS=32 # maxmium number of workers per query (postgres, singlestore, and tidb; monetdb workers are set when starting the server)

# override the driver if the script receives an argument
if [[ $# -gt 0 ]]; then
    DRIVER=$1
fi

if ! [[ "$DRIVER" =~ ^(tique|monetdb|monetdbsinglekey|postgres|singlestore|tidb) ]]; then
    echo "Invalid driver: $DRIVER"
    exit 1
fi

# update postgres workers for the olap only tests (monetdb workers are set when starting the server)
sed -i -E "s/set max_parallel_workers_per_gather = [[:digit:]]+/set max_parallel_workers_per_gather = $(($MAX_WORKERS - 1))/" chbench.py

# update singlestore workers for the olap only tests
query_parallelism_per_leaf_core=$(echo "import os; print($MAX_WORKERS / os.cpu_count())" | python3)
sed -i -E "s/set session query_parallelism_per_leaf_core = .*?/set session query_parallelism_per_leaf_core = $query_parallelism_per_leaf_core')/" chbench.py

# update tidb workers for the olap only tests
sed -i -E "s/set session tidb_distsql_scan_concurrency = [[:digit:]]+/set session tidb_distsql_scan_concurrency = $MAX_WORKERS/" chbench.py
sed -i -E "s/set session tidb_executor_concurrency = [[:digit:]]+/set session tidb_executor_concurrency = $MAX_WORKERS/" chbench.py
sed -i -E "s/set session tidb_max_tiflash_threads = [[:digit:]]+/set session tidb_max_tiflash_threads = $MAX_WORKERS/" chbench.py

# populate
python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 1 -t 1 --populate > /dev/null

# small oltp run
python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 8 -t 60 --no-load > /dev/null

# create OLAP indexes
python3 chbench.py -c 0 -t 0 $DRIVER --no-execute > /dev/null

# analyze postgres
if [ "$DRIVER" = "postgres" ]; then
    python3 analyze_pg.py
# analyze singlestore
elif [ "$DRIVER" = "singlestore" ]; then
    python3 analyze_singlestore.py
# analyze tidb 
elif [ "$DRIVER" = "tidb" ]; then
    python3 analyze_tidb.py
# analyze monetdb
else
    python3 analyze_monetdb.py
fi

# warmup run
python3 chbench.py -c 1 -r 1 -q 1 $DRIVER > /dev/null
sleep 30

echo "driver,warehouses,scale,clients,throughput,count,duration"

for c in ${CLIENTS[@]}; do
    python3 chbench.py -c $c -r $ROUNDS -q $RUNS_PER_QUERY $DRIVER | echo "$DRIVER,$WAREHOUSES,$SCALE,$c,$(grep -Po "(?<=csv::).*")"
    sleep 30
done
