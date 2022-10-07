#!/bin/bash

workdir=$1
shift

DOCKER_IMAGE="ghcr.io/cmu-delphi/covidcast-indicators-backfill_corrections"
DOCKER_TAG="latest"

# Call like:
# ./docker_run.sh "/home/indicators/runtime/backfill_corrections" "make pipeline"
docker run \
    -v "${workdir}"/logs:/backfill_corrections/logs \
    -v "${workdir}"/receiving:/backfill_corrections/receiving \
    -v "${workdir}"/params.json:/backfill_corrections/params.json \
    -v "${workdir}"/input:/backfill_corrections/input \
    -e TRAIN_PREDICT="${TRAIN_PREDICT}" \
    -i --rm "${DOCKER_IMAGE}:${DOCKER_TAG}" /bin/bash -c "$1"
