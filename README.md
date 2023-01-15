# mirror-git-to-s3 ![Build Status](https://github.com/uktrade/mirror-git-to-s3/actions/workflows/tests.yml/badge.svg?branch=main)

Python functions and CLI to mirror public git repositories available on HTTP(S) to S3. Essentially converts smart protocol git repositories to the so-called dumb protocol. Does not use temporary disk space, and uses streaming under the hood. This should allow the mirroring to be run on systems that don't have much disk or available memory, even on repositories with large objects.

This project mirrors objects stored on Large File Storage (LFS). Note however LFS objects are not accessible via the dumb protocol. To work around this, you can use [git-lfs-http-mirror](https://github.com/uktrade/git-lfs-http-mirror)  that fires up a temporary local LFS server during git commands.

At the time of writing repositories with many objects can be slow to mirror.


## Installation

```bash
pip install mirror-git-to-s3
```


## Usage

To mirror one or more repositories from Python, use the `mirror_repos` function, passing it an iterable of (source, target) mappings.

```python
from mirror_git_to_s3 import mirror_repos

mirror_repos((
    ('https://example.test/my-first-repo', 's3://my-bucket/my-first-repo'),
    ('https://example.test/my-second-repo', 's3://my-bucket/my-second-repo'),
))
```

Once a repository is mirrored to a bucket that doesn't need authentication to read, it can be cloned using standard git commands using the [virtual host or path of the S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/VirtualHosting.html).

```bash
git clone https://my-bucket.s3.eu-west-2.amazonaws.com/my-first-repo
````

Under the hood, boto3 is used to communicate with S3. The boto3 client is constructed automatically, but you can override the default by using the `get_s3_client` argument.

```python
import boto3
from mirror_git_to_s3 import mirror_repos

mirror_repos(mappings(), get_s3_client=lambda: boto3.client('s3'))
```

This can be used to mirror to S3-compatible storage.

```python
import boto3
from mirror_git_to_s3 import mirror_repos

mirror_repos(mappings(), get_s3_client=lambda: boto3.client('s3', endpoint_url='http://my-host.com/'))
```

To mirror repositories from the the command line pairs of `--source` `--target` options can be passed to `mirror-git-to-s3`.

```bash
mirror-git-to-s3 \
    --source 'https://example.test/my-first-repo' --target 's3://my-bucket/my-first-repo' \
    --source 'https://example.test/my-second-repo' --target 's3://my-bucket/my-second-repo'
```

At the time of writing, there is no known standard way of discovering a set of associated git repositories, hence to remain general, this project must be told the source and target addresses of each repository explicitly.


## Under the hood

The project requests a [git packfile](https://git-scm.com/book/en/v2/Git-Internals-Packfiles) with all objects from each source, separates it into its component [git objects](https://git-scm.com/book/en/v2/Git-Internals-Git-Objects), and stores each separately in S3 as an S3 object.

- The packfile is requested via a single POST request. Its response is stream-processed to avoid loading it all into memory at once.
- An attempt is made to split processing of the response into separate threads where possible. Stream processing is somewhat at-odds with parallel processing, but there are still parts that can be moved to separate threads.
- Where parallism is not possible, for example when a delta object in the packfile has to wait for its base object, a [threading.Event](https://docs.python.org/3/library/threading.html#event-objects) is used for the thread to wait.
- So far a deadlock has not been observed, but this depends on the packfile being returned in an order where base objects are always before the deltas that depend on them.
