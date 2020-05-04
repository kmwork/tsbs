#!/bin/sh -v
clear
./tsbs_generate_data --KostyaСountOfColumns=5000 \
    --use-case="devops" --seed=500 --scale=1000 \
    --timestamp-start="2020-04-01T00:00:00Z" \
    --timestamp-end="2020-04-01T00:01:00Z" \
    --log-interval="30s" --format="clickhouse" \
    --KostyaСountOfColumns=15000 \
    | gzip > /my-ext4/yandex-data-t4.gz
