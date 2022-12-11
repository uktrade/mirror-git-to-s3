# mirror-git-to-s3

Python functions and CLI to mirror git repositories to S3. Essentially converts smart protocol git repositories to the so-called dumb protocol. Does not use disk, and uses streaming under the hood. This should allow this to be run on systems that don't have much disk to available memory, even on large repositories.

> Work in progress. This README serves as a rough design spec.
