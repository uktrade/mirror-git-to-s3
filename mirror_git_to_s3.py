import itertools
import zlib
import urllib.parse
from hashlib import sha1
from queue import SimpleQueue
from struct import unpack
from threading import Lock, Event, Thread

import boto3
import httpx


def mirror_repos(mappings,
        get_http_client=lambda: httpx.Client(transport=httpx.HTTPTransport(retries=3)),
        get_s3_client=lambda: boto3.client('s3'),
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

    def construct_object_from_ref_delta(s3_client, bucket, target_prefix, shas, base_sha, delta_bytes):
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

        object_type, _ = shas[base_sha]

        return object_type, target_size, yield_object_bytes()

    def clear_tmp(s3_client, bucket, target_prefix):
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=f'{target_prefix}/mirror_tmp/'):
            items = [
                {'Key': item['Key']}
                for item in page.get('Contents', [])
            ]
            if items:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': items})

    def get_pack_objects(s3_client, http_client, bucket, target_prefix, base_url, shas):
        r = http_client.request('GET', f'{base_url}/info/refs?service=git-upload-pack')
        r.raise_for_status()

        sha_refs = [
            line.split()
            for line in r.content.splitlines()[2:-1]
        ]
        pack_file_request = b''.join(
            b'0032want ' + sha[4:] + b'\n'
            for sha, ref in sha_refs
        ) + b'0000' + b'0009done\n'

        got_object_names = {}
        with http_client.stream('POST', f'{base_url}/git-upload-pack', content=pack_file_request) as response:
            r.raise_for_status()
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
                yield \
                    (object_type, object_length, yield_with_asserted_length(uncompress_zlib(yield_indefinite, return_unused), object_length)) if object_type in (1, 2, 3, 4) else \
                    construct_object_from_ref_delta(s3_client, bucket, target_prefix, shas, base_sha=read_bytes(20), delta_bytes=yield_with_asserted_length(uncompress_zlib(yield_indefinite, return_unused), object_length))

            trailer = read_bytes(20)

    types_names_for_hash = {
        1: b'commit',
        2: b'tree',
        3: b'blob',
        4: b'tag',
    }

    s3_client = get_s3_client()
    with get_http_client() as http_client:
        for source_base_url, target in mappings:
            shas = dict()

            parsed_target = urllib.parse.urlparse(target)
            assert parsed_target.scheme == 's3'
            bucket = parsed_target.netloc
            target_prefix = parsed_target.path[1:] # Remove leading /
            clear_tmp(s3_client, bucket, target_prefix)

            try:
                for object_type, object_length, object_bytes in get_pack_objects(s3_client, http_client, bucket, target_prefix, source_base_url, shas):
                    binary_prefix = types_names_for_hash[object_type] + b' ' + str(object_length).encode() + b'\x00'
                    sha = sha1(binary_prefix)
                    temp_file_name =  f'{target_prefix}/mirror_tmp/1'
                    s3_client.upload_fileobj(to_filelike_obj(yield_with_sha(object_bytes, sha)), Bucket=bucket, Key=temp_file_name)
                    sha_hex = sha.hexdigest()
                    try:
                        s3_client.copy(CopySource={
                            'Bucket': bucket,
                            'Key': temp_file_name,
                        }, Bucket=bucket, Key=f'{target_prefix}/mirror_tmp/raw/{sha_hex}')
                    finally:
                        s3_client.delete_object(Bucket=bucket, Key=temp_file_name)

                    shas[sha.digest()] = (object_type, object_length)

                    # Upload in prefixed and compressed format to final location
                    resp = s3_client.get_object(Bucket=bucket, Key=f'{target_prefix}/mirror_tmp/raw/{sha_hex}')
                    s3_client.upload_fileobj(to_filelike_obj(compress_zlib(itertools.chain((binary_prefix,), resp['Body']))), Bucket=bucket, Key=f'{target_prefix}/objects/{sha_hex[0:2]}/{sha_hex[2:]}')
            finally:
                clear_tmp(s3_client, bucket, target_prefix)

    print('End')
