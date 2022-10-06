docker run \
    -v "${workdir}"/logs:/backfill_corrections/logs \
    -v "${workdir}"/receiving:/backfill_corrections/receiving \
    -v "${workdir}"/params.json:/backfill_corrections/params.json \
    -v "${workdir}"/input:/backfill_corrections/input \
    -i --rm "${DOCKER_IMAGE}:${DOCKER_TAG}" /bin/bash -c "$1"
