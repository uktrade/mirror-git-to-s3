#!/bin/sh

set -e

docker run --rm -p 9000:9000 --name mirror-git-to-s3-minio -d \
  -e 'MINIO_ACCESS_KEY=AKIAIDIDIDIDIDIDIDID' \
  -e 'MINIO_SECRET_KEY=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
  -e 'MINIO_REGION=us-east-1' \
  -e 'MINIO_VOLUMES=/data{1...4}' \
  -e 'MINIO_CI_CD=1' \
  minio/minio:RELEASE.2022-11-11T03-44-20Z \
  server
