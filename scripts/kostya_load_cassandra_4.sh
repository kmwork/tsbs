#!/bin/bash
clear
export PATH=$PATH:/home/lin/go/bin
# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_cassandra)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_cassandra not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATABASE_PORT=${DATABASE_PORT:-9042}
DATA_FILE_NAME=yandex-and-cassandra-data-4cols.gz
export BULK_DATA_DIR=/my-ext4


# Load parameters - personal
CASSANDRA_TIMEOUT=${CASSANDRA_TIMEOUT:-60000s}
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}
BATCH_SIZE=10

EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

while ! nc -z ${DATABASE_HOST} ${DATABASE_PORT}; do
    echo "Waiting for cassandra"
    sleep 1
done

cqlsh -e 'drop keyspace measurements;'
cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --KostyaСountOfColumns=4 \
                                --workers=1 \
                                --batch-size=${BATCH_SIZE} \
                                --reporting-period=${REPORTING_PERIOD} \
                                --write-timeout=${CASSANDRA_TIMEOUT} \
                                --hosts=${DATABASE_HOST}:${DATABASE_PORT} \
                                --replication-factor=${REPLICATION_FACTOR}
