#!/bin/sh -v
./tsbs_generate_data --use-case="devops" --seed=5 --scale=10 \
    --timestamp-start="2020-04-01T00:00:00Z" \
    --timestamp-end="2020-04-01T00:01:00Z" \
    --log-interval="30s" --format="cassandra" \
    | gzip > /my-ext4/cassandra-data-t2.gz

#./tsbs_generate_queries --use-case="devops" --seed=500 --scale=5000 \
#    --timestamp-start="2019-01-01T00:00:00Z" \
#    --timestamp-end="2020-04-20T00:00:00Z" \
#    --queries=5000 --query-type="single-groupby-1-1-1" --format="clickhouse" \
#    | gzip > /tmp/clickhouse-queries-seed500.gz

#FORMATS="clickhouse" SCALE=5000 SEED=500 \
#    TS_START="2019-01-01T00:00:00Z" \
#    TS_END="2020-04-20T00:00:00Z" \
#    QUERIES=1000 QUERY_TYPES="last-loc low-fuel avg-load" \
#    BULK_DATA_DIR="/tmp/clickhouse-seed500-bulk_queries" scripts/generate_queries.sh


#FORMATS="clickhouse" SCALE=5000 SEED=500 \
#    TS_START="2019-01-01T00:00:00Z" \
#    TS_END="2020-04-20T00:00:00Z" \
#    QUERIES=5000 QUERY_TYPES="single-groupby-1-1-1" \
#    BULK_DATA_DIR="/my-ext4/clickhouse-seed500-bulk_queries"./generate_queries.sh
