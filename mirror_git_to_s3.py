import itertools
import re
import zlib
import uuid
import urllib.parse
from collections import defaultdict
from functools import partial
from hashlib import sha1
from queue import SimpleQueue, Queue
from struct import unpack
from threading import Lock, Event, Thread

import boto3
import click
import httpx


def mirror_repos(mappings,
        get_http_client=lambda: httpx.Client(transport=httpx.HTTPTransport(retries=3)),
        get_s3_client=lambda: boto3.client('s3'),
        num_object_workers=10,
        num_lfs_workers=10,
        lfs_queue_size=10000,  # A queue item is small
    ):

    def next_or_truncated_error(it):
        try:
            return next(it)
        except StopIteration:
            raise Exception('Truncated') from None

    def get_reader(bytes_iter):
        chunk = b''
        offset = 0
        it = iter(bytes_iter)

        def _read(num_bytes):
            nonlocal chunk, offset

            while num_bytes:
                if offset == len(chunk):
                    try:
                        chunk = next_or_truncated_error(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num_bytes, len(chunk) - offset)
                offset = offset + to_yield
                num_bytes -= to_yield
                yield chunk[offset - to_yield:offset]

        def yield_indefinite(num_bytes=float('infinity')):
            yield from _read(num_bytes)

        def read_bytes(num_bytes):
            return b''.join(_read(num_bytes))

        def return_unused(num_unused):
            nonlocal offset
            offset -= num_unused

        return yield_indefinite, read_bytes, return_unused

    def smooth(bytes_iter, interval=1.0):
        # Due to deltas, our streaming processing can have large sections when we don't
        # fetch any data, and the remote can think we have gone away. To avoid, we make
        # sure to pull every interval
        it = iter(bytes_iter)
        thread_exception = None
        get_from_thread_exception = None
        fetch_next = Event()
        queue = SimpleQueue()
        lock = Lock()
        amount_in_queue = 0

        def pull():
            nonlocal amount_in_queue, fetch_next, thread_exception

            try:
                while True:
                    regular = fetch_next.wait(timeout=interval)
                    fetch_next.clear()
                    try:
                        chunk = next(it)
                    except StopIteration:
                        queue.put(done)
                        break
                    len_chunk = len(chunk)
                    with lock:
                        amount_in_queue += len_chunk
                        to_report = amount_in_queue  # To not print when holding the lock
                    if not regular:
                        print('Forced fetch to avoid HTTP timeout on server. In queue:', to_report)
                    queue.put(chunk)
                    chunk = None
            except Exception as e:
                thread_exception = e

        t = Thread(target=pull)
        t.start()

        try:
            fetch_next.set()
            while chunk := queue.get(timeout=60):
                if chunk is done:
                    break
                len_chunk = len(chunk)
                with lock:
                    amount_in_queue -= len_chunk
                if queue.empty():
                    fetch_next.set()
                yield chunk
                chunk = None
            t.join(timeout=60)
        except Exception as e:
            get_from_thread_exception = e

        if thread_exception is not None:
            raise thread_exception
        elif get_from_thread_exception is not None:
            raise get_from_thread_exception

    def uncompress_zlib(read_indefinite, return_unused):
        dobj = zlib.decompressobj()

        for compressed_chunk in read_indefinite():
            uncompressed_chunk = dobj.decompress(compressed_chunk)
            if uncompressed_chunk:
                yield uncompressed_chunk

            while dobj.unconsumed_tail and not dobj.eof:
                uncompressed_chunk = dobj.decompress(dobj.unconsumed_tail)
                if uncompressed_chunk:
                    yield uncompressed_chunk

            if dobj.eof:
                return_unused(len(dobj.unused_data))
                break

    def compress_zlib(chunks):
        cobj = zlib.compressobj()
        for chunk in chunks:
            compressed_chunk = cobj.compress(chunk)
            if compressed_chunk:
                yield compressed_chunk
        if compressed_chunk := cobj.flush():
            yield compressed_chunk

    def to_filelike_obj(iterable):
        chunk = b''
        offset = 0
        it = iter(iterable)

        def up_to_iter(num):
            nonlocal chunk, offset

            while num:
                if offset == len(chunk):
                    try:
                        chunk = next(it)
                    except StopIteration:
                        break
                    else:
                        offset = 0
                to_yield = min(num, len(chunk) - offset)
                offset = offset + to_yield
                num -= to_yield
                yield chunk[offset - to_yield:offset]

        class FileLikeObj:
            def read(self, n=-1):
                n = \
                    n if n != -1 else \
                    float('infinity')
                return b''.join(up_to_iter(n))

        return FileLikeObj()

    def get_object_type_and_length(read_bytes):
        b = read_bytes(1)[0]

        t = (b >> 4) & 7
        length = (b & 15)
        bits_to_shift_length = 4

        while b & 128:
            b = read_bytes(1)[0]
            length += (b & 127) << bits_to_shift_length
            bits_to_shift_length += 7

        return t, length

    def get_length(read_bytes):
        bits_to_shift_length = 0
        b = 128  # To enter the loop
        length = 0

        while b & 128:
            b = read_bytes(1)[0]
            length += (b & 127) << bits_to_shift_length
            bits_to_shift_length += 7

        return length

    def yield_with_asserted_length(bytes_iter, expected_length):
        length = 0
        for chunk in bytes_iter:
            length += len(chunk)
            yield chunk
        assert length == expected_length

    def yield_with_sha(bytes_iter, sha):
        for chunk in bytes_iter:
            sha.update(chunk)
            yield chunk

    def yield_with_lfs(bytes_iter):
        search_for = b'version https://git-lfs.github.com/spec/v1\n'
        lfs_pointer_raw = b''

        def _to_yield():
            nonlocal lfs_pointer_raw

            for chunk in bytes_iter:
                if len(lfs_pointer_raw) < len(search_for) or lfs_pointer_raw[:len(search_for)] == search_for:
                    lfs_pointer_raw += chunk
                yield chunk

        def _is_lfs():
            return lfs_pointer_raw[:len(search_for)] == search_for

        def _lfs_pointer():
            lines = lfs_pointer_raw.splitlines()
            return lines[1].split(b':')[1].decode(), int(lines[2].split()[1].decode())

        return _to_yield(), _is_lfs, _lfs_pointer

    def queue_to_iterable(queue):
        while value := queue.get():
            if value is done:
                break
            yield value

    def clear_tmp(s3_client, bucket, target_prefix):
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=f'{target_prefix}/mirror_tmp/'):
            items = [
                {'Key': item['Key']}
                for item in page.get('Contents', [])
            ]
            if items:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': items})

    def get_refs(http_client, base_url):
        r = http_client.request('GET', f'{base_url}/info/refs?service=git-upload-pack')
        r.raise_for_status()
        lines = r.content.splitlines()
        head_ref = re.match(b'.*symref=HEAD:(\\S+).*', lines[1]).group(1)
        return head_ref, [
            line.split()
            for line in lines[2:-1]
        ]

    def upload_object(http_client, bucket, base_url, object_type, object_length, sha_lock, sha_events, shas, object_bytes):
        binary_prefix = types_names_for_hash[object_type] + b' ' + str(object_length).encode() + b'\x00'
        sha = sha1(binary_prefix)
        temp_file_name =  f'{target_prefix}/mirror_tmp/{str(uuid.uuid4())}'
        with_sha = yield_with_sha(object_bytes, sha)
        with_lfs_check, is_lfs, lfs_pointer = yield_with_lfs(with_sha)
        s3_client.upload_fileobj(to_filelike_obj(with_lfs_check), Bucket=bucket, Key=temp_file_name)
        sha_hex = sha.hexdigest()
        s3_client.copy(CopySource={
            'Bucket': bucket,
            'Key': temp_file_name,
        }, Bucket=bucket, Key=f'{target_prefix}/mirror_tmp/raw/{sha_hex}')

        with sha_lock:
            shas[sha.digest()] = object_type
            sha_events[sha.digest()].set()

        s3_client.delete_object(Bucket=bucket, Key=temp_file_name)

        # Upload in prefixed and compressed format to final location
        resp = s3_client.get_object(Bucket=bucket, Key=f'{target_prefix}/mirror_tmp/raw/{sha_hex}')
        s3_client.upload_fileobj(to_filelike_obj(compress_zlib(itertools.chain((binary_prefix,), resp['Body']))), Bucket=bucket, Key=f'{target_prefix}/objects/{sha_hex[0:2]}/{sha_hex[2:]}')

        if not is_lfs():
            return
        lfs_sha256, lfs_size = lfs_pointer()
        lfs_queue.put(partial(upload_lfs, s3_client, http_client, bucket, target_prefix, base_url, lfs_sha256, lfs_size))

    def construct_object_from_delta_and_upload(s3_client, bucket, base_url, target_prefix, sha_lock, sha_events, shas, base_sha, delta_bytes):
        yield_indefinite, read_bytes, return_unused = get_reader(delta_bytes)
        base_size = get_length(read_bytes)
        target_size = get_length(read_bytes)

        def read_sparse(instruction, instruction_bit_range):
            value = 0
            factor = 0
            for b in instruction_bit_range:
                has = (instruction >> b) & 1
                if has:
                    value += read_bytes(1)[0] << factor
                factor += 8
            return value

        def yield_object_bytes():
            target_size_remaining = target_size
            while target_size_remaining:
                instruction = read_bytes(1)[0]
                assert instruction != 0
                if instruction >> 7:
                    offset = read_sparse(instruction, range(0, 4))
                    size = read_sparse(instruction, range(4, 7)) or 65536
                    sha_hex = base_sha.hex()
                    resp = s3_client.get_object(Bucket=bucket, Key=f'{target_prefix}/mirror_tmp/raw/{sha_hex}', Range='bytes={}-{}'.format(offset, offset + size - 1))
                    target_size_remaining -= size
                    yield from yield_with_asserted_length(resp['Body'], size)
                else:
                    size = instruction & 127
                    target_size_remaining -= size
                    yield from yield_indefinite(size)

            # Not expecting any bytes - this is to exhaust the iterator to put back bytes after zlib
            for _ in delta_bytes:
                pass

        # Wait to make sure the base object has been uploaded
        with sha_lock:
            sha_event = sha_events[base_sha]
        sha_event.wait()
        with sha_lock:
            object_type = shas[base_sha]

        upload_object(http_client, bucket, base_url, object_type, target_size, sha_lock, sha_events, shas, yield_object_bytes())

    def yield_lfs_data(http_client, bucket, base_url, lfs_sha256, lfs_size):
        batch_response = http_client.post(base_url.removesuffix('.git') + '.git/info/lfs/objects/batch', json={
            'operation': 'download',
            'objects': [{'oid': lfs_sha256, 'size': lfs_size}]
        })
        batch_response.raise_for_status()
        download_href = batch_response.json()['objects'][0]['actions']['download']['href']

        with httpx.stream('GET', download_href) as bytes_response:
            yield from bytes_response.iter_bytes()

    def upload_lfs(s3_client, http_client, bucket, target_prefix, base_url, lfs_sha256, lfs_size):
        print('Uploading LFS', lfs_sha256, lfs_size)
        key = f'{target_prefix}/lfs/objects/' + lfs_sha256[0:2] + '/' + lfs_sha256[2:4] + '/' + lfs_sha256
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise
        else:
            print('LFS exists, skipping', lfs_sha256)
            return
        lfs_data = yield_lfs_data(http_client, bucket, base_url, lfs_sha256, lfs_size)
        s3_client.upload_fileobj(to_filelike_obj(lfs_data), Bucket=bucket, Key=key)
        print('Uploaded', lfs_sha256, lfs_size)

    def worker_func(q):
        while item := q.get():
            try:
                if item is done:
                    break
                item()
            except Exception as e:
                print('Exception in thread', e)
            finally:
                q.task_done()

    done = object()
    types_names_for_hash = {
        1: b'commit',
        2: b'tree',
        3: b'blob',
        4: b'tag',
    }

    s3_client = get_s3_client()

    with get_http_client() as http_client:
        for source_base_url, target in mappings:
            # Process objects and LFS files in separate threads so (when possible) to minimise blocking
            object_queue = Queue(maxsize=1)
            object_workers = [Thread(target=worker_func, args=(object_queue,)) for _ in range(0, num_object_workers)]
            for worker in object_workers:
                worker.start()

            lfs_queue = Queue(maxsize=lfs_queue_size)
            lfs_workers = [Thread(target=worker_func, args=(lfs_queue,)) for _ in range(0, num_lfs_workers)]
            for worker in lfs_workers:
                worker.start()

            sha_lock = Lock()
            sha_events = defaultdict(Event)
            shas = {}

            parsed_target = urllib.parse.urlparse(target)
            assert parsed_target.scheme == 's3'
            bucket = parsed_target.netloc
            target_prefix = parsed_target.path[1:] # Remove leading /
            clear_tmp(s3_client, bucket, target_prefix)

            head_ref, refs = get_refs(http_client, source_base_url)

            try:
                pack_file_request = b''.join(
                    b'0032want ' + sha[4:] + b'\n'
                    for sha, ref in refs
                ) + b'0000' + b'0009done\n'

                with http_client.stream('POST', f'{source_base_url}/git-upload-pack', content=pack_file_request) as response:
                    response.raise_for_status()

                    yield_indefinite, read_bytes, return_unused = get_reader(smooth(response.iter_bytes(16384)))

                    length = int(read_bytes(4), 16)
                    chunk = read_bytes(length - 4)
                    assert chunk == b'NAK\n'

                    signature = read_bytes(4)
                    assert signature == b'PACK'
                    version, = unpack('>I', read_bytes(4))
                    assert version == 2

                    number_of_objects, = unpack('>I', read_bytes(4))

                    for i in range(0, number_of_objects):
                        object_type, object_length = get_object_type_and_length(read_bytes)
                        assert object_type in (1, 2, 3, 4, 7)  # 6 == OBJ_OFS_DELTA is unsupported for now

                        uncompressed = yield_with_asserted_length(uncompress_zlib(yield_indefinite, return_unused), object_length)
                        object_bytes_queue = Queue(maxsize=1)

                        object_queue.put(
                            partial(upload_object, http_client, bucket, source_base_url, object_type, object_length, sha_lock, sha_events, shas, object_bytes=queue_to_iterable(object_bytes_queue,)) if object_type in (1, 2, 3, 4) else \
                            partial(construct_object_from_delta_and_upload, s3_client, bucket, source_base_url, target_prefix, sha_lock, sha_events, shas, base_sha=read_bytes(20), delta_bytes=queue_to_iterable(object_bytes_queue,))
                        )
                        for chunk in uncompressed:
                            object_bytes_queue.put(chunk)
                        object_bytes_queue.put(done)

                    trailer = read_bytes(20)

                    print('Waiting for regular objects to be uploaded')
                    for i in range(0, num_object_workers):
                        object_queue.put(done)
                    for worker in object_workers:
                        worker.join()

                    # Ensure all LFS workers have finished
                    print('Regular objects uploaded. Waiting for LFS objects to be copied')
                    for i in range(0, num_lfs_workers):
                        lfs_queue.put(done)
                    for worker in lfs_workers:
                        worker.join()
                    print('LFS objects uploaded')

                    s3_client.put_object(Bucket=bucket, Key=f'{target_prefix}/HEAD', Body=b'ref: ' + head_ref)
                    s3_client.put_object(Bucket=bucket, Key=f'{target_prefix}/info/refs', Body=b''.join(sha[4:] + b'\t' + ref + b'\n' for sha, ref in refs))
            finally:
                clear_tmp(s3_client, bucket, target_prefix)

    print('End')


@click.command()
@click.option('--source', '-s', multiple=True, required=True)
@click.option('--target', '-s', multiple=True, required=True)
def main(source, target):
    mirror_repos(zip(source, target))


if __name__ == '__main__':
    main()
