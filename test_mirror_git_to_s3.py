from mirror_git_to_s3 import mirror_repos


def test():
	mirror_repos('https://github.com/uktrade/mirror-git-to-s3.git')
