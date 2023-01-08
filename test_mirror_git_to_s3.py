import uuid
import subprocess
import tempfile
import boto3
import botocore

from mirror_git_to_s3 import mirror_repos


def test():
    bucket_name = 'my-bucket'

    s3_client = boto3.client('s3',
        endpoint_url='http://127.0.0.1:9000/',
        aws_access_key_id='AKIAIDIDIDIDIDIDIDID',
        aws_secret_access_key='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
        region_name='us-east-1',
    )
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket_name):
        items = [
            {'Key': item['Key']}
            for item in page.get('Contents', [])
        ]
        if items:
            s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': items})

    mirror_repos((
        ('https://github.com/uktrade/mirror-git-to-s3.git', f's3://{bucket_name}/mirror-git-to-s3'),
        # ('https://github.com/uktrade/data-workspace.git',  f's3://{bucket_name}/data-workspace'),
    ), get_s3_client=lambda: s3_client)

    with tempfile.TemporaryDirectory() as tmpdir:
        completed = subprocess.run(["git", "clone", f"http://localhost:9000/{bucket_name}/mirror-git-to-s3", tmpdir])

    assert completed.returncode == 0
