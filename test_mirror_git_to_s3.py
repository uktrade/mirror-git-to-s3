import uuid
import boto3

from mirror_git_to_s3 import mirror_repos


def test():
	bucket_name = uuid.uuid4().hex

	s3_client = boto3.client('s3',
		endpoint_url='http://127.0.0.1:9000/',
		aws_access_key_id='AKIAIDIDIDIDIDIDIDID',
		aws_secret_access_key='aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
		region_name='us-east-1',
	)
	s3_client.create_bucket(Bucket=bucket_name)

	mirror_repos((
		('https://github.com/uktrade/mirror-git-to-s3.git', f's3://{bucket_name}/mirror-git-to-s3'),
		# ('https://github.com/uktrade/data-workspace.git',  f's3://{bucket_name}/data-workspace'),
	), get_s3_client=lambda: s3_client)
