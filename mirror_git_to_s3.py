import zlib
import urllib.parse
from hashlib import sha1
from struct import unpack

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

        def read_indefinite():
            yield from _read(float('infinity'))

        def read_bytes(num_bytes):
            return b''.join(_read(num_bytes))

        def return_unused(num_unused):
            nonlocal offset
            offset -= num_unused

        return read_indefinite, read_bytes, return_unused

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

    def get_pack_objects(http_client, base_url):
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
        needed_object_names = set()
        with http_client.stream('POST', f'{base_url}/git-upload-pack', content=pack_file_request) as response:
            r.raise_for_status()
            read_indefinite, read_bytes, return_unused = get_reader(response.iter_bytes())

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
                assert object_type in (1, 2, 3, 4, 6, 7)

                if object_type in (1, 2, 3, 4):
                    yield object_type, object_length, None, None, yield_with_asserted_length(uncompress_zlib(read_indefinite, return_unused), object_length)

                elif object_type == 6:  # OBJ_OFS_DELTA
                    offset = get_length(read_bytes)
                    yield object_type, object_length, offset, None, yield_with_asserted_length(uncompress_zlib(read_indefinite, return_unused), object_length)

                else:  # OBJ_REF_DELTA
                    object_name = read_bytes(20)
                    needed_object_names.add(object_name)
                    yield object_type, object_length, None, object_name, yield_with_asserted_length(uncompress_zlib(read_indefinite, return_unused), object_length)

            trailer = read_bytes(20)

    types_names_for_hash = {
        1: b'commit',
        2: b'tree',
        3: b'blob',
        4: b'tag',
        6: b'',  # delta - hash not calculated
        7: b'',  # delta - hash not calculated
    }

    s3_client = get_s3_client()
    with get_http_client() as http_client:
        for source_base_url, target in mappings:
            parsed_target = urllib.parse.urlparse(target)
            assert parsed_target.scheme == 's3'
            bucket = parsed_target.netloc
            target_prefix = parsed_target.path[1:] # Remove leading /

            for object_type, object_length, delta_offset, delta_ref, object_bytes in get_pack_objects(http_client, source_base_url):
                sha = sha1(types_names_for_hash[object_type] + b' ' + str(object_length).encode() + b'\x00')

                temp_file_name =  f'{target_prefix}/mirror_tmp/1'
                def with_sha():
                    for chunk in object_bytes:
                        sha.update(chunk)
                        yield chunk
                sha_hex = sha.hexdigest()

                s3_client.upload_fileobj(to_filelike_obj(with_sha()), Bucket=bucket, Key=temp_file_name)
                response = s3_client.copy(CopySource={
                    'Bucket': bucket,
                    'Key': temp_file_name,
                }, Bucket=bucket, Key=f'{target_prefix}/objects/{sha_hex[0:2]}/{sha_hex[2:]}')
                s3_client.delete_object(Bucket=bucket, Key=temp_file_name)

    print('End')
