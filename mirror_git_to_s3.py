import zlib
from hashlib import sha1
from struct import unpack

import httpx

def mirror_repos(base_url):

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

    with httpx.Client(transport=httpx.HTTPTransport(retries=3)) as http_client:
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

                    types_names = {
                        1: b'commit',
                        2: b'tree',
                        3: b'blob',
                        4: b'tag',
                    }

                    object_bytes = b''.join(uncompress_zlib(read_indefinite, return_unused))
                    sha = sha1(types_names[object_type] + b' ' + str(len(object_bytes)).encode() + b'\x00' + object_bytes).digest()
                    got_object_names[sha] = object_type
                    # if object_type == 4:
                    #     print('BASE', object_type, object_bytes[0:100])
                    #     print('SHA', object_type, sha)
                    assert len(object_bytes) == object_length

                elif object_type == 6:  # OBJ_OFS_DELTA
                    length = get_length(read_bytes)
                    object_bytes = b''.join(uncompress_zlib(read_indefinite, return_unused))
                    assert len(object_bytes) == length

                else:  # OBJ_REF_DELTA
                    object_name = read_bytes(20)
                    # print("NAME", object_name)
                    needed_object_names.add(object_name)
                    object_bytes = b''.join(uncompress_zlib(read_indefinite, return_unused))
                    assert len(object_bytes) == object_length

            trailer = read_bytes(20)

        print(len(needed_object_names), len(got_object_names))
        print('got types')
        for n in needed_object_names:
            if n in got_object_names:
                print('got', got_object_names[n])
        # print('Needed and not got', needed_object_names - got_object_names)

        # pack_file_request = b''.join(
        #     b'0032want ' + sha.hex().encode() + b'\n'
        #     for sha in (needed_object_names - got_object_names)
        # ) + b'0000' + b'0009done\n'

        # with http_client.stream('POST', f'{base_url}/git-upload-pack', content=pack_file_request) as response:
        #     r.raise_for_status()
        #     print(r.content)

        print('End')
