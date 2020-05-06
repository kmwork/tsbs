#!/bin/bash
clear
export PATH=$PATH:/home/lin/go/bin
# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_clickhouse)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_clickhouse not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=yandex-and-cassandra-data-5000cols.gz
DATABASE_USER=${DATABASE_USER:-default}
DATABASE_PASSWORD=1
export BULK_DATA_DIR=/my-ext4
# Load parameters - personal
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-10s}
HASH_WORKERS=${HASH_WORKERS:-false}
export BATCH_SIZE=10
EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --KostyaСountOfColumns=5000 \
                                --host="mmk-i1-clickhouse.datana.ru" \
                                --user=${DATABASE_USER} \
                                --password="Gbplfnsq!" \
                                --port=9000 \
                                --db-name=${DATABASE_NAME} \
                                --batch-size=${BATCH_SIZE} \
                                --workers=1 \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --hash-workers=${HASH_WORKERS}
