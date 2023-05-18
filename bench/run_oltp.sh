# Configs
DRIVER=tique # (tique | monetdb | monetdbsinglekey (recommended for native monetbd) | postgres | singlestore | tidb)
WAREHOUSES=512
SCALE=1
TIME=60
CLIENTS=(1 2 4 8 16 32)
CONTENTION=(0) # if > 0, executes the contention test with the provided number of warehouses

# override the driver if the script receives an argument
if [[ $# -gt 0 ]]; then
    DRIVER=$1
fi

if ! [[ "$DRIVER" =~ ^(tique|monetdb|monetdbsinglekey|postgres|singlestore|tidb) ]]; then
    echo "Invalid driver: $DRIVER"
    exit 1
fi

# populate
python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 1 -t 1 --populate > /dev/null

# analyze
if [ "$DRIVER" = "postgres" ]; then
    python3 analyze_pg.py
elif [ "$DRIVER" = "singlestore" ]; then
    python3 analyze_singlestore.py
elif [ "$DRIVER" = "tidb" ]; then
    python3 analyze_tidb.py
else
    python3 analyze_monetdb.py
fi

# warmup run
python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 8 -t 60 --no-load > /dev/null
# (due to the collisions, it is better to warmup the regular monetdb with just one client)
#python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c 1 -t 60 --no-load > /dev/null
sleep 30

echo "driver,warehouses,scale,clients,contention,throughput,abort_rate"

for c in ${CLIENTS[@]}; do
    for contention in ${CONTENTION[@]}; do
        python3 tpcc.py $DRIVER -w $WAREHOUSES -s $SCALE -c $c -t $TIME --no-load --contention $contention \
            | echo "$DRIVER,$WAREHOUSES,$SCALE,$c,$contention,$(grep -Po "(?<=csv::).*")"
        sleep 30
    done
done
