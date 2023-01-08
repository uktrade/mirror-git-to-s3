#!/bin/sh

set -e

docker network create minio || true

docker run --rm -p 9000:9000 --name mirror-git-to-s3-minio -d \
  --network=minio \
  -e 'MINIO_ACCESS_KEY=AKIAIDIDIDIDIDIDIDID' \
  -e 'MINIO_SECRET_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  -e 'MINIO_REGION=us-east-1' \
  -e 'MINIO_VOLUMES=/data{1...4}' \
  -e 'MINIO_CI_CD=1' \
  minio/minio:RELEASE.2022-11-11T03-44-20Z \
  server

timeout 60 bash -c 'until echo > /dev/tcp/127.0.0.1/9000; do sleep 5; done'

docker run --rm \
  --network=minio \
  --entrypoint '' \
  minio/mc \
  sh -c '
    mc alias set my-minio http://mirror-git-to-s3-minio:9000/ AKIAIDIDIDIDIDIDIDID aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa &&
    mc mb my-minio/my-bucket &&
    mc anonymous set download my-minio/my-bucket
  '
