#!/bin/sh -v
clear
./tsbs_generate_data --use-case="devops" --seed=500 --scale=1000 \
    --timestamp-start="2020-04-01T00:00:00Z" \
    --timestamp-end="2020-04-01T00:01:00Z" \
    --log-interval="30s" --format="clickhouse" \
    | gzip > /my-ext4/yandex-data-t4.gz
