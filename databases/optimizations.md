#### All Systems
- Create appropriate indexes for TPC-C and CH-benchmark.

#### MonetDB/TiQuE
- Use the `minimal_fast` optimizer in the transactional workloads;
- Run the `ANALYZE` command after populating;
- TiQuE only: tables *Item*, *Nation*, *Region*, and *Supplier* are not transformed, as they static.

#### PostgreSQL
- Increase `shared_buffers` to the recommended value of 25% of the system memory;
- Increase `max_wal_size` to 10GB;
- Increase `max_worker_processes` to 32 (OLAP);
- Increase `max_parallel_workers` to 32 (OLAP);
- Increase `max_paralle_workers_per_gather` to 32 (OLAP);
- Disable `jit` (in the workloads used, we found it to negatively impact the response time);
- Run the `VACUUM ANALYZE` command after populating;
- Other parameters tried:
  - Increasing `work_mem` in several GBs resulted in the same analytical performance.

#### SingleStore
- Increase `maximum_memory` to also consider swap (to avoid analytical queries crashing due to out-of-memory errors);
- Set `query_parallelism_per_leaf_core` to -1 (OLAP);
- Define *Warehouse* and *District* tables as row-based, as they are only used in the transactional workloads;
- Define sort keys for the tables with range queries; 
- Run the `sdb-admin optimize` command before starting the server;
- Run the `ANALYZE TABLE` and `OPTIMIZE TABLE ... FULL` commands after populating;
- Other parameters tried, but showed either no improvements or a regression in performance:
  - `columnstore_flush_bytes` (up to 1GB);
  - `columnstore_disk_insert_threshold` (no change after 16MB);
  - `columnstore_segment_rows` (performance regression with 4k and 10M);
  - `enable_background_plan_invalidation` (not needed as the data distribution remains mostly the same);
  - `snapshot_trigger_size` (up to 10GB);
  - `log_file_size_partitions` (up to 1GB);
  - `log_file_size_ref_dbs` (up to 1GB);

#### TiDB
- Increase `tidb_distsql_scan_concurrency` to 32 (OLAP); 
- Increase `tidb_executor_concurrency` to 32 (OLAP);
- Increase `tidb_max_tiflash_threads` to 32 (OLAP);
- Set `tidb_mem_quota_query` to -1 to avoid out-of-memory errors (OLAP);
- Set `tidb_isolation_read_engines` to `tiflash,tidb` (OLAP);
- Set `tidb_server_memory_limit` to 0 to avoid out-of-memory errors;
- Set `tidb_txn_mode` as `optimistic` by default (results in a 8% average increase in transactional performance);
- Add query hints to use the TiKV engine in the transactional results (results in a lower response time);
- Do not replicate tables *Warehouse*, *District*, and *History*, as they are not used in the analytical tests;
- Run the `ANALYZE TABLE` command after populating;
- Other parameters tried, but showed either no improvements or a regression in performance:
  - `tidb_opt_agg_push_down` (regression in OLAP);
  - `tidb_enable_outer_join_reorder`;
  - `tidb_enable_parallel_apply`;
  - `tidb_index_join_batch_size`;
  - `tidb_index_lookup_size`;
  - `tidb_opt_distinct_agg_push_down`.
