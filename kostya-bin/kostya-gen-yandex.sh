#!/bin/sh -v
./tsbs_generate_data --use-case="devops" --seed=5 --scale=10 \
    --timestamp-start="2020-04-01T00:00:00Z" \
    --timestamp-end="2020-04-01T00:01:00Z" \
    --log-interval="30s" --format="cassandra" \
    | gzip > /my-ext4/cassandra-data-t2.gz
