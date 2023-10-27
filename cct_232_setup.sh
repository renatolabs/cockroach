#!/usr/bin/env bash

set -eux

CLUSTER=cct-232
USERNAME=cct
NUM_NODES=${NUM_NODES:-15}
CRDB_NODES=1-$((NUM_NODES-1))
WORKLOAD_NODE=$NUM_NODES
MACHINE_TYPE=n2-standard-8
VOLUME_SIZE=1000 # 1 TB
LIFETIME=2160h   # 90 days
ZONES=us-east1-b,us-east1-c,us-east1-d

# We bootstrap the cluster in the previous version to test upgrades
# and mixed-version states.
INITIAL_VERSION=v23.1.11

#### CLUSTER SETUP START ####

# Build roachprod to make sure we have the latest version.
./dev build roachprod

# Build cockroach.
./dev build cockroach --cross

# Create the cluster itself.
./bin/roachprod create ${CLUSTER} \
                --clouds gce \
                --nodes ${NUM_NODES} \
                --gce-machine-type ${MACHINE_TYPE} \
                --gce-pd-volume-size ${VOLUME_SIZE} \
                --local-ssd=false \
                --gce-zones ${ZONES} \
                --label "usage=cct-232" \
                --username $USERNAME \
                --lifetime $LIFETIME

# Stage the initial version binary in the cockroach nodes.
./bin/roachprod run $CLUSTER:$CRDB_NODES mkdir $INITIAL_VERSION
./bin/roachprod stage $CLUSTER:$CRDB_NODES release $INITIAL_VERSION --dir $INITIAL_VERSION

# Start the cluster
./bin/roachprod start $CLUSTER:$CRDB_NODES \
                --binary $INITIAL_VERSION/cockroach \
                --args "--config-profile=replication-source" \
                --schedule-backups \
                --secure

# Use the most recent `cockroach` binary for the workload node.
./bin/roachprod put $CLUSTER:$WORKLOAD_NODE ./artifacts/cockroach ./cockroach

# Enable node_exporter on all nodes
./bin/roachprod grafana-start $CLUSTER

#### CLUSTER SETUP END ####

#### TPCC WORKLOAD START ####

PGURLS=$(./bin/roachprod pgurl --secure $CLUSTER:$CRDB_NODES)

TPCC_WAREHOUSES=12000
TPCC_DB=cct_tpcc
# TODO: run with user/password (non-root)

cat <<EOF >/tmp/tpcc_init.sh
#!/usr/bin/env bash

./cockroach workload init tpcc \
    --warehouses $TPCC_WAREHOUSES \
    --db $TPCC_DB \
    --secure \
    --families \
    $PGURLS
EOF

./bin/roachprod put $CLUSTER:$WORKLOAD_NODE /tmp/tpcc_init.sh tpcc_init.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE chmod +x tpcc_init.sh

cat <<EOF >/tmp/tpcc_run.sh
#!/usr/bin/env bash

while true; do
    echo ">> Starting tpcc workload"
    ./cockroach workload run tpcc \
        --warehouses $TPCC_WAREHOUSES \
        --concurrency 128 \
        --db $TPCC_DB \
        --secure \
        --ramp 10m \
        --display-every 5s \
        --duration 12h
        --families \
        $PGURLS

    sleep 1
EOF

./bin/roachprod put $CLUSTER:$WORKLOAD_NODE /tmp/tpcc_run.sh tpcc_run.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE chmod +x tpcc_run.sh

# Initialize and start workload.
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE ./tpcc_init.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE -- sudo systemd-run --service-type exec --collect --unit cct_tpcc ./tpcc_run.sh

#### TPCC WORKLOAD END ####

#### KV WORKLOAD START ####

KV_DB=cct_kv
# TODO: run with user/password (non-root)

KV_PERFDIR=/mnt/data1/kv_perf

./bin/roachprod run $CLUSTER:$WORKLOAD_NODE mkdir -p $KV_PERFDIR

cat <<EOF >/tmp/kv_run.sh
#!/usr/bin/env bash

while true; do
    echo ">> Starting kv workload"
    ./cockroach workload run kv \
        --init \
        --drop \
        --concurrency 128 \
        --histograms ${KV_PERFDIR}/stats.json \
        --db $KV_DB \
        --splits 1000 \
        --span-percent 90 \
        --cycle-length 5000 \
        --min-block-bytes 100 \
        --max-block-bytes 1000 \
        --max-rate 2000 \
        --secure \
        --ramp 10m \
        --display-every 5s \
        --duration 12h
        --enum \
        $PGURLS

    sleep 1
EOF

./bin/roachprod put $CLUSTER:$WORKLOAD_NODE /tmp/kv_run.sh kv_run.sh
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE chmod +x kv_run.sh

# Start workload.
./bin/roachprod run $CLUSTER:$WORKLOAD_NODE -- sudo systemd-run --service-type exec --collect --unit cct_kv ./kv_run.sh

#### KV WORKLOAD END ####
