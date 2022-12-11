# mirror-git-to-s3

Python functions and CLI to mirror git repositories available on HTTP(S) to S3. Essentially converts smart protocol git repositories to the so-called dumb protocol. Does not use disk, and uses streaming under the hood. This should allow this to be run on systems that don't have much disk to available memory, even on large repositories.

> Work in progress. This README serves as a rough design spec.


## Usage

To mirror one or more repositories from Python, use the `mirror_repos` function, passing it an iterable of (source, target) mappings.

```python
from mirror_git_to_s3 import mirror_repos

mirror_repos((
	('https://example.test/my-first-repo', 's3://my-bucket/my-first-repo'),
	('https://example.test/my-second-repo', 's3://my-bucket/my-second-repo'),
))
```

In the previous example the iterable is itself a tuple. However in general any iterable is supported, Under the hood repositories are processed in parallel, and transfers can start before the entire list is known.

```python
from mirror_git_to_s3 import mirror_repos

def mappings():
	yield ('https://example.test/my-first-repo', 's3://my-bucket/my-first-repo')
	yield ('https://example.test/my-second-repo', 's3://my-bucket/my-second-repo')

mirror_repos(mappings())
```

To mirror repositories from the the command line pairs of `--source` `--target` options can be passed to `mirror-git-to-s3`.

```bash
mirror-git-to-s3 \
	--source 'https://example.test/my-first-repo' --target 's3://my-bucket/my-first-repo' \
	--source 'https://example.test/my-second-repo' --target 's3://my-bucket/my-second-repo'
```

At the time of writing, there is no known standard way of discovering a set of associated git repositories, hence to remain general, this project must be told the source and target addresses of each repository explicitly.
