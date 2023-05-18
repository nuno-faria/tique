# Configs
DRIVER=tique # (tique | monetdb | monetdbsinglekey (recommended for native monetbd) | postgres | singlestore | tidb)
WAREHOUSES=512
SCALE=1
OLTP_CLIENTS=(6)
OLAP_CLIENTS=(0 1 2 3 4 5 6)
TIME=180
FINISH_BY_TIME=true # true or false; if true, OLAP and OLTP take TIME to run; else, we run both OLAP and OLTP until one OLAP round is completed 
CROSS_CLIENTS=true # true or false; if true, each OLTP_CLIENTS number is tested with each OLAP_CLIENTS number; else, the first OLAP_CLIENTS is tested with the first OLTP_CLIENTS, the second OLAP_CLIENTS with the second OLAP_CLIENTS, and so on
MAX_WORKERS=4 # to setup the number of parallel workers in PostgreSQL, SingleStore, and TiDB (MonetDB must be set when starting the server)
KEEP_EXECUTING=false # force the idle olap clients to keep executing queries. used for the hybrid tests with increasing oltp and olap, to get more realistic results
mkdir -p results

# override the driver if the script receives an argument
if [[ $# -gt 0 ]]; then
    DRIVER=$1
fi

if ! [[ "$DRIVER" =~ ^(tique|monetdb|monetdbsinglekey|postgres|singlestore|tidb) ]]; then
    echo "Invalid driver: $DRIVER"
    exit 1
fi

# update postgres workers for the hybrid tests
# (max_parallel_workers_per_gather = 0 means that one thread will be used)
sed -i -E "s/set max_parallel_workers_per_gather = [[:digit:]]+/set max_parallel_workers_per_gather = $(($MAX_WORKERS - 1))/" chbench.py

# update singlestore workers for the hybrid tests
query_parallelism_per_leaf_core=$(echo "import os; print($MAX_WORKERS / os.cpu_count())" | python3)
sed -i -E "s/set session query_parallelism_per_leaf_core = .*?/set session query_parallelism_per_leaf_core = $query_parallelism_per_leaf_core')/" chbench.py

# update tidb workers for the olap only tests
sed -i -E "s/set session tidb_distsql_scan_concurrency = [[:digit:]]+/set session tidb_distsql_scan_concurrency = $MAX_WORKERS/" chbench.py
sed -i -E "s/set session tidb_executor_concurrency = [[:digit:]]+/set session tidb_executor_concurrency = $MAX_WORKERS/" chbench.py
sed -i -E "s/set session tidb_max_tiflash_threads = [[:digit:]]+/set session tidb_max_tiflash_threads = $MAX_WORKERS/" chbench.py

## populate
python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 1 -t 1 --populate > /dev/null

# create OLAP indexes
python3 chbench.py -c 0 -t 0 $DRIVER --no-execute > /dev/null

# prepare postgres
if [ "$DRIVER" = "postgres" ]; then
    python3 analyze_pg.py
# prepare singlestore
elif [ "$DRIVER" = "singlestore" ]; then
    python3 analyze_singlestore.py
# prepare tidb
elif [ "$DRIVER" = "tidb" ]; then
    python3 analyze_tidb.py
#prepare monetdb
else
    python3 analyze_monetdb.py
fi

# warmup runs
python3 chbench.py -c 2 -t 30 $DRIVER > /dev/null &
OLAP_PID=$!
python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 8 -t 60 --soft-reset > /dev/null &
OLTP_PID=$!
tail --pid=$OLAP_PID -f /dev/null
tail --pid=$OLTP_PID -f /dev/null
sleep 30

# run
run() {    
    if [ "$1" -gt 0 ]; then
        if [ $FINISH_BY_TIME = true ]; then
            python3 chbench.py -c $1 -t $TIME $DRIVER > results/olap.txt &
        else
            if [ $KEEP_EXECUTING = true ]; then
                python3 chbench.py -c $1 -r 1 $DRIVER --keep-executing > results/olap.txt &
            else
                python3 chbench.py -c $1 -r 1 $DRIVER > results/olap.txt &
            fi
        fi
        OLAP_PID=$!
    fi
    if [ "$2" -gt 0 ]; then
        if [ $FINISH_BY_TIME = true ]; then
            python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c $2 -t $TIME --no-load > results/oltp.txt &
        else
            python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c $2 -t 999999 --no-load > results/oltp.txt &
        fi
        OLTP_PID=$!
    fi

    # wait
    if [ "$1" -gt 0 ]; then
        tail --pid=$OLAP_PID -f /dev/null
        olap_result="$(grep -Poa '(?<=Throughput: )\d+(\.\d+)?' results/olap.txt),$(grep -Poa '(?<=Count: )\d+(\.\d+)?' results/olap.txt),$(grep -Poa '(?<=Total duration: )\d+(\.\d+)?' results/olap.txt)"
    else
        olap_result='0'
    fi
    if [ "$2" -gt 0 ]; then
        if ! [ $FINISH_BY_TIME = true ]; then
            kill -15 $OLTP_PID
        fi
        tail --pid=$OLTP_PID -f /dev/null
        oltp_result=$(grep -Poa '(?<=Throughput: )\d+(\.\d+)?' results/oltp.txt)
    else
        oltp_result='0'
    fi

    echo "$DRIVER,$WAREHOUSES,$SCALE,$2,$1,$oltp_result,$olap_result"
    
    sleep 30
}

echo "driver,wh,scale,oltp_c,olap_c,oltp_txs,olap_txs,olap_count,olap_duration"

if [ $CROSS_CLIENTS = true ]; then
    for olap_c in ${OLAP_CLIENTS[@]}; do
        for oltp_c in ${OLTP_CLIENTS[@]}; do
            run $olap_c $oltp_c 
        done
    done
else
    s1=${#OLAP_CLIENTS[@]}
    s2=${#OLTP_CLIENTS[@]}
    s=$((s1 > s2 ? s2 : s1))
    for ((i=0;i<s;i++)); do
        run ${OLAP_CLIENTS[$i]} ${OLTP_CLIENTS[$i]}
    done
fi
