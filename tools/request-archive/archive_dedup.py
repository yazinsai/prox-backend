"""Lossless immutable manifests and shared Zstandard chunks for request archives."""
from compression import zstd
from contextlib import contextmanager, ExitStack
import hashlib
import io
import json
import os
from pathlib import Path
import re
import tempfile
import shutil
import zipfile
from collections import OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor


MANIFEST_SUFFIX = '.json.zst'
MANIFEST_MAX_BYTES = 16 * 1024 * 1024
MAX_CHUNKS = 200000
MAX_PACK_MEMBERS = 200000
MAX_CHUNK_BYTES = 64 * 1024 * 1024
MAX_COMPRESSED_PAYLOAD_BYTES = MAX_CHUNK_BYTES + 1024 * 1024
MAX_COUNTER = (1 << 63) - 1
MAX_SEMANTIC_RECORD_BYTES = 256 * 1024 * 1024
DEFAULT_CHUNK_BYTES = 32 * 1024
DEFAULT_STRING_CHUNK_BYTES = 2 * 1024 * 1024
DEFAULT_STRING_BYTES = 512
DEFAULT_LEVEL = 19
HASH_PATTERN = re.compile(r'[0-9a-f]{64}\Z')
# Possessive groups bound matching work even for very large escaped strings.
# Matches are byte spans only; neither JSON parsing nor reserialization occurs.
JSON_STRING = re.compile(rb'"(?:[^"\\]++|\\.)*+"')
_PACK_INDEX = {}
_PACK_GENERATIONS = {}
_PACK_INVENTORY_DEPTH = 0
_MANIFEST_METADATA = {}
_MANIFEST_METADATA_DIRECTORY = None
MAX_MANIFEST_METADATA = 100000
MAX_DICTIONARY_BYTES = 1024 * 1024
MAX_DICTIONARIES = 16
_DICTIONARIES = OrderedDict()
DICTIONARY_POINTERS = ('active-dictionary', 'active-manifest-dictionary')
COMPRESSION_WORKERS = 4
COMPRESSION_PREFETCH = 8
_COMPRESSION_POOL = ThreadPoolExecutor(max_workers=COMPRESSION_WORKERS)


def sync_directory(directory):
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def ensure_directory(directory):
    if directory.is_symlink():
        raise ValueError('Archive storage directory must not be a symlink')
    if directory.exists():
        if not directory.is_dir():
            raise ValueError('Archive storage path is not a directory')
        return
    directory.mkdir(mode=0o700)
    sync_directory(directory.parent)


def manifest_path(directory, name):
    if Path(name).name != name or '\\' in name or '..' in name or not name.endswith('.log.zst'):
        raise ValueError('Invalid logical archive name')
    return directory / 'request-records' / (name + MANIFEST_SUFFIX)


def validate_digest(digest):
    if not isinstance(digest, str) or HASH_PATTERN.fullmatch(digest) is None:
        raise ValueError('Invalid archive chunk digest')


def chunk_path(directory, digest):
    validate_digest(digest)
    return directory / 'request-chunks' / digest[:2] / (digest + '.zst')


def validate_manifest(value, name=None):
    if not isinstance(value, dict) or type(value.get('schema_version')) is not int or value['schema_version'] != 1:
        raise ValueError('Unsupported archive manifest')
    record_name = value.get('name')
    if not isinstance(record_name, str):
        raise ValueError('Invalid archive manifest name')
    manifest_path(Path('.'), record_name)
    if name is not None and record_name != name:
        raise ValueError('Archive manifest name mismatch')
    for key in ('modified_ns', 'original_bytes'):
        if type(value.get(key)) is not int or not 0 <= value[key] <= MAX_COUNTER:
            raise ValueError('Invalid archive manifest counter')
    if type(value.get('legacy_compressed_bytes', 0)) is not int or not 0 <= value.get('legacy_compressed_bytes', 0) <= MAX_COUNTER:
        raise ValueError('Invalid archive manifest counter')
    validate_digest(value.get('sha256'))
    chunks = value.get('chunks')
    if not isinstance(chunks, list) or len(chunks) > MAX_CHUNKS:
        raise ValueError('Invalid archive manifest chunks')
    size = 0
    for chunk in chunks:
        if not isinstance(chunk, dict):
            raise ValueError('Invalid archive manifest chunk')
        validate_digest(chunk.get('hash'))
        if type(chunk.get('size')) is not int or not 0 < chunk['size'] <= MAX_CHUNK_BYTES:
            raise ValueError('Invalid archive chunk size')
        size += chunk['size']
    if size != value['original_bytes']:
        raise ValueError('Archive manifest size mismatch')
    return value


def read_manifest(path, name=None):
    if path.parent.is_symlink():
        raise ValueError('Archive manifest directory must not be a symlink')
    with open_payload(path) as compressed, decoded_payload(path.parent.parent, compressed) as source:
        body = source.read(MANIFEST_MAX_BYTES + 1)
    if len(body) > MANIFEST_MAX_BYTES:
        raise ValueError('Archive manifest exceeds limit')
    return validate_manifest(json.loads(body), name)


def manifest_metadata(path, name, stamp=None):
    """Cache scalar listing metadata only; reconstruction always rereads JSON."""
    global _MANIFEST_METADATA_DIRECTORY
    directory = path.parent
    if _MANIFEST_METADATA_DIRECTORY != directory or len(_MANIFEST_METADATA) >= MAX_MANIFEST_METADATA:
        _MANIFEST_METADATA.clear()
        _MANIFEST_METADATA_DIRECTORY = directory
    stamp = payload_stamp(path) if stamp is None else stamp
    cached = _MANIFEST_METADATA.get(path)
    if cached is not None and cached[0] == stamp:
        return cached[1]
    manifest = read_manifest(path, name)
    metadata = {key: manifest[key] for key in
                ('name', 'modified_ns', 'original_bytes', 'sha256', 'legacy_compressed_bytes')
                if key in manifest}
    _MANIFEST_METADATA[path] = (stamp, metadata)
    return metadata


def split_chunks(data, chunk_bytes=DEFAULT_CHUNK_BYTES, string_bytes=DEFAULT_STRING_BYTES,
                 string_chunk_bytes=DEFAULT_STRING_CHUNK_BYTES):
    """Anchor long strings so repeated images/context survive offsets changing.

    Arbitrary/binary data and gaps use bounded fixed chunks. A raw JSON string
    above the threshold is chunked relative to its own start, not its offset
    within a growing request. Every byte is retained, including quote/escape
    spelling, headers, separators and invalid JSON.
    """
    if not 0 < chunk_bytes <= MAX_CHUNK_BYTES or not 0 < string_chunk_bytes <= MAX_CHUNK_BYTES or string_bytes < 1:
        raise ValueError('Invalid archive chunk settings')
    start = 0
    view = memoryview(data)
    for match in JSON_STRING.finditer(data):
        if match.end() - match.start() < string_bytes:
            continue
        for begin, end, limit in ((start, match.start(), chunk_bytes),
                                  (match.start(), match.end(), string_chunk_bytes)):
            for offset in range(begin, end, limit):
                yield view[offset:min(end, offset + limit)]
        start = match.end()
    for offset in range(start, len(data), chunk_bytes):
        yield view[offset:min(len(data), offset + chunk_bytes)]


def read_chunk(directory, chunk):
    path = chunk_path(directory, chunk['hash'])
    if path.parent.is_symlink() or path.parent.parent.is_symlink():
        raise ValueError('Archive chunk directory must not be a symlink')
    with open_payload(path) as compressed, decoded_payload(directory, compressed) as source:
        body = source.read(chunk['size'] + 1)
    if len(body) != chunk['size'] or hashlib.sha256(body).hexdigest() != chunk['hash']:
        raise ValueError('Archive chunk failed verification')
    return body


def open_private_file(path):
    if path.is_symlink():
        raise ValueError('Archive payload must not be a symlink')
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    return os.fdopen(fd, 'rb')


def dictionary_root(directory):
    root = directory / 'request-chunks'
    dictionaries = root / 'dictionaries'
    if root.is_symlink() or dictionaries.is_symlink():
        raise ValueError('Archive dictionary directory must not be a symlink')
    return dictionaries


def load_dictionary(directory, dictionary_id):
    if type(dictionary_id) is not int or not 0 < dictionary_id < 1 << 32:
        raise ValueError('Invalid archive dictionary ID')
    path = dictionary_root(directory) / (str(dictionary_id) + '.zdict')
    if path.is_symlink():
        raise ValueError('Archive dictionary must not be a symlink')
    stat = path.stat()
    stamp = (stat.st_ino, stat.st_size, stat.st_mtime_ns)
    key = (directory, dictionary_id)
    cached = _DICTIONARIES.get(key)
    if cached is not None and cached[0] == stamp:
        _DICTIONARIES.move_to_end(key)
        return cached[1]
    with open_private_file(path) as source:
        body = source.read(MAX_DICTIONARY_BYTES + 1)
    if len(body) > MAX_DICTIONARY_BYTES:
        raise ValueError('Archive dictionary exceeds limit')
    try:
        dictionary = zstd.ZstdDict(body)
    except (ValueError, zstd.ZstdError) as error:
        raise ValueError('Invalid archive dictionary') from error
    if dictionary.dict_id != dictionary_id:
        raise ValueError('Archive dictionary ID mismatch')
    _DICTIONARIES[key] = (stamp, dictionary)
    _DICTIONARIES.move_to_end(key)
    while len(_DICTIONARIES) > MAX_DICTIONARIES:
        _DICTIONARIES.popitem(last=False)
    return dictionary


def active_dictionary(directory, pointer_name='active-dictionary'):
    if pointer_name not in DICTIONARY_POINTERS:
        raise ValueError('Invalid archive dictionary pointer')
    path = dictionary_root(directory) / pointer_name
    try:
        with open_private_file(path) as source:
            pointer = source.read(64)
    except FileNotFoundError:
        return None
    value = pointer.strip()
    if not value.isdigit() or len(value) > 10:
        raise ValueError('Invalid active archive dictionary')
    return load_dictionary(directory, int(value))


def install_dictionary(directory, source_path, pointer_name='active-dictionary'):
    if pointer_name not in DICTIONARY_POINTERS:
        raise ValueError('Invalid archive dictionary pointer')
    with open_private_file(source_path) as source:
        body = source.read(MAX_DICTIONARY_BYTES + 1)
    if len(body) > MAX_DICTIONARY_BYTES:
        raise ValueError('Archive dictionary exceeds limit')
    try:
        dictionary = zstd.ZstdDict(body)
    except (ValueError, zstd.ZstdError) as error:
        raise ValueError('Invalid archive dictionary') from error
    if not 0 < dictionary.dict_id < 1 << 32:
        raise ValueError('Archive dictionary requires a nonzero ID')
    ensure_directory(directory / 'request-chunks')
    dictionaries = dictionary_root(directory)
    ensure_directory(dictionaries)
    target = dictionaries / (str(dictionary.dict_id) + '.zdict')
    if not publish_bytes(target, body):
        with open_private_file(target) as existing:
            if existing.read(MAX_DICTIONARY_BYTES + 1) != body:
                raise ValueError('Archive dictionary ID collision')
    load_dictionary(directory, dictionary.dict_id)
    pointer = dictionaries / pointer_name
    fd, temporary = tempfile.mkstemp(prefix='.request-dictionary-', dir=dictionaries)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write((str(dictionary.dict_id) + '\n').encode('ascii'))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, pointer)
        sync_directory(dictionaries)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return dictionary


@contextmanager
def decoded_payload(directory, compressed):
    info = zstd.get_frame_info(compressed.read(18))
    compressed.seek(0)
    dictionary = load_dictionary(directory, info.dictionary_id) if info.dictionary_id else None
    with zstd.open(compressed, 'rb', zstd_dict=dictionary,
                   options={zstd.DecompressionParameter.window_log_max: 26}) as source:
        yield source


def packed_index(directory):
    """Cache immutable pack member locations; directory changes invalidate it."""
    if directory.is_symlink():
        raise ValueError('Archive payload directory must not be a symlink')
    cached = _PACK_INDEX.get(directory)
    if _PACK_INVENTORY_DEPTH and cached is not None:
        return cached[1]
    try:
        directory_stat = directory.stat()
        generation = (directory_stat.st_ino, directory_stat.st_size, directory_stat.st_mtime_ns)
    except FileNotFoundError:
        generation = None
    if cached is not None and _PACK_GENERATIONS.get(directory) == generation:
        return cached[1]
    signature = []
    pattern = 'records-*.zip' if directory.name == 'request-records' else 'chunks-*.zip'
    for path in sorted(directory.glob(pattern)):
        if path.is_symlink():
            raise ValueError('Archive pack must not be a symlink')
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        signature.append((path.name, stat.st_ino, stat.st_size, stat.st_mtime_ns))
    signature = tuple(signature)
    if cached is not None and cached[0] == signature:
        _PACK_GENERATIONS[directory] = generation
        return cached[1]
    entries = {}
    for pack_name, _, _, _ in signature:
        path = directory / pack_name
        try:
            with open_private_file(path) as file, zipfile.ZipFile(file) as pack:
                if len(pack.infolist()) > MAX_PACK_MEMBERS:
                    raise ValueError('Archive pack exceeds member limit')
                for member in pack.infolist():
                    name = member.filename
                    if Path(name).name != name or '\\' in name or '..' in name or not name.endswith('.zst'):
                        raise ValueError('Invalid packed archive payload name')
                    if member.compress_type != zipfile.ZIP_STORED or member.file_size > MAX_COMPRESSED_PAYLOAD_BYTES:
                        raise ValueError('Invalid archive pack member encoding or size')
                    if directory.name == 'request-records':
                        manifest_path(Path('.'), name.removesuffix(MANIFEST_SUFFIX))
                        if not name.endswith(MANIFEST_SUFFIX) or member.file_size > MANIFEST_MAX_BYTES + 1024 * 1024:
                            raise ValueError('Invalid packed request manifest')
                    elif HASH_PATTERN.fullmatch(name.removesuffix('.zst')) is None or name[:2] != directory.name:
                        raise ValueError('Invalid packed archive chunk digest')
                    if name in entries:
                        raise ValueError('Duplicate packed archive payload')
                    entries[name] = (path, member.CRC, member.file_size)
        except FileNotFoundError:
            continue
    # Entries contain paths/metadata only, so hundreds of shard caches do not
    # retain open file descriptors.
    _PACK_INDEX[directory] = (signature, entries)
    _PACK_GENERATIONS[directory] = generation
    return entries


@contextmanager
def inventory_snapshot():
    """Freeze pack inventory while an optimizer holds its exclusive lock.

    Own pack publication and missing-file retries explicitly invalidate caches.
    Loose payload existence still uses filesystem metadata on every check.
    """
    global _PACK_INVENTORY_DEPTH
    if _PACK_INVENTORY_DEPTH == 0:
        # Force one signature check per directory instead of discarding entries:
        # packs are immutable, so an unchanged signature proves the entries current
        # and avoids reopening every pack on each watcher cycle.
        _PACK_GENERATIONS.clear()
    _PACK_INVENTORY_DEPTH += 1
    try:
        yield
    finally:
        _PACK_INVENTORY_DEPTH -= 1
        if _PACK_INVENTORY_DEPTH == 0:
            # Directory metadata may have changed during frozen operation;
            # normal readers refresh each touched directory on their next call.
            _PACK_GENERATIONS.clear()


def payload_exists(path):
    return path.name in packed_index(path.parent) or path.exists()


def payload_stamp(path):
    location = packed_index(path.parent).get(path.name)
    if location is not None:
        pack_path, crc, size = location
        stat = pack_path.stat()
        return (str(pack_path), stat.st_ino, stat.st_size, stat.st_mtime_ns, crc, size)
    stat = path.stat()
    return (stat.st_ino, stat.st_size, stat.st_mtime_ns)


@contextmanager
def open_payload(path):
    """Open an immutable compressed payload, preferring a published pack."""
    if path.is_symlink():
        raise ValueError('Archive payload must not be a symlink')
    for attempt in range(2):
        with ExitStack() as stack:
            location = packed_index(path.parent).get(path.name)
            try:
                if location is not None:
                    file = stack.enter_context(open_private_file(location[0]))
                    pack = stack.enter_context(zipfile.ZipFile(file))
                    source = stack.enter_context(pack.open(path.name))
                else:
                    source = stack.enter_context(open_private_file(path))
            except FileNotFoundError:
                if attempt:
                    raise
                _PACK_INDEX.pop(path.parent, None)
                continue
            yield source
            return


def payload_paths(directory, suffix):
    names = set(packed_index(directory))
    names.update(path.name for path in directory.glob('*' + suffix))
    return [directory / name for name in sorted(names) if name.endswith(suffix)]


def payload_metadata_locations(directory, suffix):
    """Snapshot physical identities once per listing, rather than per member."""
    packed = packed_index(directory)
    pack_stats = {}
    locations = {}
    for name, (path, crc, size) in packed.items():
        if not name.endswith(suffix):
            continue
        if path not in pack_stats:
            stat = path.stat()
            pack_stats[path] = (str(path), stat.st_ino, stat.st_size, stat.st_mtime_ns)
        locations[name] = (directory / name, (*pack_stats[path], crc, size))
    for path in directory.glob('*' + suffix):
        if path.name in locations:
            continue
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        locations[path.name] = (path, (stat.st_ino, stat.st_size, stat.st_mtime_ns))
    return [locations[name] for name in sorted(locations)]


def pack_payloads(directory, prefix, suffix, batch_size, validator, all_tails=False, progress=None):
    """Pack already-compressed objects; publication precedes verified cleanup."""
    if not directory.exists():
        return 0
    packed = packed_index(directory)
    # A crash can leave source files after their immutable pack was published.
    for name in packed:
        path = directory / name
        if not path.exists():
            continue
        with open_payload(path) as stored, open_private_file(path) as loose:
            while True:
                a, b = stored.read(1024 * 1024), loose.read(1024 * 1024)
                if a != b:
                    raise ValueError('Conflicting packed archive source; retained file')
                if not a:
                    break
        validator(path)
        path.unlink()
    sync_directory(directory)
    count = 0
    while True:
        candidates = sorted(directory.glob('*' + suffix))[:batch_size]
        if not candidates or (len(candidates) < batch_size and not all_tails):
            return count
        fd, temporary = tempfile.mkstemp(prefix='.request-object-pack-', dir=directory)
        snapshots = {}
        try:
            with os.fdopen(fd, 'w+b') as output:
                with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_STORED, allowZip64=True) as pack:
                    for path in candidates:
                        if progress is not None:
                            progress(count)
                        before = path.stat()
                        validator(path)
                        snapshots[path.name] = (before.st_ino, before.st_size, before.st_mtime_ns)
                        member = zipfile.ZipInfo(path.name)
                        member.external_attr = 0o100600 << 16
                        # Bounded members do not need Zip64 local-header extras.
                        # allowZip64 still supports large pack offsets/archives.
                        member.file_size = before.st_size
                        with open_private_file(path) as source, pack.open(member, 'w') as target:
                            shutil.copyfileobj(source, target, 1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
            # Validate copied compressed bytes independently before publication.
            with zipfile.ZipFile(temporary) as pack:
                for path in candidates:
                    with open_private_file(path) as source, pack.open(path.name) as copied:
                        while True:
                            a, b = source.read(1024 * 1024), copied.read(1024 * 1024)
                            if a != b:
                                raise ValueError('Archive pack copy failed verification')
                            if not a:
                                break
            destination = directory / (prefix + Path(temporary).name.removeprefix('.request-object-pack-') + '.zip')
            os.link(temporary, destination)
            sync_directory(directory)
            _PACK_INDEX.pop(directory, None)
            for path in candidates:
                after = path.stat()
                if snapshots[path.name] != (after.st_ino, after.st_size, after.st_mtime_ns):
                    raise ValueError('Archive pack source changed; retained file')
                path.unlink()
            sync_directory(directory)
            count += len(candidates)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


def validate_chunk_file(directory, path):
    digest = path.name.removesuffix('.zst')
    chunk_path(directory, digest)
    with open_payload(path) as compressed, decoded_payload(directory, compressed) as source:
        body = source.read(MAX_CHUNK_BYTES + 1)
    if len(body) > MAX_CHUNK_BYTES or hashlib.sha256(body).hexdigest() != digest:
        raise ValueError('Archive chunk pack source failed verification')


def compact_storage(directory, all_tails=False, progress=None):
    """Compact shared payloads without recompressing or changing references."""
    # Bulk optimization minimizes pack headers and allocation slack. The watcher
    # uses smaller thresholds so newly captured objects do not accumulate loose.
    manifest_batch = 5000 if all_tails else 1000
    chunk_batch = 5000 if all_tails else 100
    manifests = pack_payloads(directory / 'request-records', 'records-', MANIFEST_SUFFIX, manifest_batch,
                              lambda path: read_manifest(path, path.name.removesuffix(MANIFEST_SUFFIX)),
                              all_tails=all_tails, progress=progress)
    chunks = 0
    root = directory / 'request-chunks'
    if root.is_symlink():
        raise ValueError('Archive chunk directory must not be a symlink')
    for shard in sorted(root.glob('*')):
        if shard.name == 'dictionaries':
            continue
        if progress is not None:
            progress(manifests + chunks)
        if not shard.is_dir():
            continue
        if shard.is_symlink() or re.fullmatch(r'[0-9a-f]{2}', shard.name) is None:
            raise ValueError('Invalid archive chunk shard')
        chunks += pack_payloads(shard, 'chunks-', '.zst', chunk_batch,
                                lambda path: validate_chunk_file(directory, path), all_tails=all_tails,
                                progress=progress)
    return {'packed_manifests': manifests, 'packed_chunks': chunks}


class RecordReader(io.RawIOBase):
    """Bounded plaintext reconstruction, validating chunks and the final record."""
    def __init__(self, directory, manifest):
        super().__init__()
        self.directory = directory
        self.manifest = validate_manifest(manifest)
        self.next_chunk = 0
        self.pending = b''
        self.offset = 0
        self.digest = hashlib.sha256()
        self.total = 0
        self.verified = False

    def readable(self):
        return True

    def readinto(self, target):
        if self.closed:
            raise ValueError('Read from closed archive')
        if not target:
            return 0
        if self.offset == len(self.pending):
            if self.next_chunk == len(self.manifest['chunks']):
                if not self.verified:
                    if self.total != self.manifest['original_bytes'] or self.digest.hexdigest() != self.manifest['sha256']:
                        raise ValueError('Archive record failed verification')
                    self.verified = True
                return 0
            self.pending = read_chunk(self.directory, self.manifest['chunks'][self.next_chunk])
            self.next_chunk += 1
            self.offset = 0
            self.digest.update(self.pending)
            self.total += len(self.pending)
        count = min(len(target), len(self.pending) - self.offset)
        target[:count] = self.pending[self.offset:self.offset + count]
        self.offset += count
        return count


@contextmanager
def open_record(directory, path, name=None):
    manifest = read_manifest(path, name)
    with io.BufferedReader(RecordReader(directory, manifest), buffer_size=1024 * 1024) as source:
        yield source


def publish_bytes(path, data):
    """Publish without replacement; durability precedes manifest/source cleanup."""
    fd, temporary = tempfile.mkstemp(prefix='.request-dedup-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        sync_directory(path.parent)
        return True
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def publish_record(directory, name, data, modified_ns, legacy_compressed_bytes,
                   chunk_bytes=DEFAULT_CHUNK_BYTES, string_bytes=DEFAULT_STRING_BYTES,
                   level=DEFAULT_LEVEL, verified_chunks=None,
                   string_chunk_bytes=DEFAULT_STRING_CHUNK_BYTES, progress=None, dictionary=None,
                   manifest_dictionary=None):
    """Publish and verify one immutable record; caller owns old-source cleanup."""
    digest = hashlib.sha256(data).hexdigest()
    return publish_chunks(directory, name,
                          split_chunks(data, chunk_bytes, string_bytes, string_chunk_bytes),
                          modified_ns, legacy_compressed_bytes, len(data), digest,
                          level, verified_chunks, progress, dictionary, manifest_dictionary)


def publish_stream_record(directory, name, source, modified_ns, legacy_compressed_bytes,
                          original_bytes, digest, chunk_bytes=DEFAULT_CHUNK_BYTES,
                          level=DEFAULT_LEVEL, verified_chunks=None, progress=None, dictionary=None,
                          manifest_dictionary=None):
    """Bound memory for exceptional records larger than the semantic scan limit."""
    def chunks():
        while chunk := source.read(chunk_bytes):
            yield chunk
    return publish_chunks(directory, name, chunks(), modified_ns, legacy_compressed_bytes,
                          original_bytes, digest, level, verified_chunks, progress, dictionary, manifest_dictionary)


def compressed_chunks(directory, source_chunks, level, dictionary):
    """Prefetch bounded compression only; filesystem publication stays serial."""
    # Reuse prepared tables for this level instead of rebuilding the dictionary
    # for every small object. Only the level and frame checksum are configured.
    compression_dictionary = dictionary.as_digested_dict if dictionary is not None else None
    pending = deque()
    hashes = {}
    iterator = iter(source_chunks)
    exhausted = False
    options = {zstd.CompressionParameter.compression_level: level,
               zstd.CompressionParameter.checksum_flag: 1}
    try:
        while pending or not exhausted:
            while len(pending) < COMPRESSION_PREFETCH and not exhausted:
                try:
                    chunk = next(iterator)
                except StopIteration:
                    exhausted = True
                    break
                reference = {'hash': hashlib.sha256(chunk).hexdigest(), 'size': len(chunk)}
                future = None
                if not payload_exists(chunk_path(directory, reference['hash'])):
                    future = hashes.get(reference['hash'])
                    if future is None:
                        future = _COMPRESSION_POOL.submit(zstd.compress, chunk, options=options,
                                                          zstd_dict=compression_dictionary)
                        hashes[reference['hash']] = future
                pending.append((chunk, reference, future))
            if pending:
                chunk, reference, future = pending.popleft()
                yield chunk, reference, future
                hashes.pop(reference['hash'], None)
    finally:
        for _, _, future in pending:
            if future is not None:
                future.cancel()


def publish_chunks(directory, name, source_chunks, modified_ns, legacy_compressed_bytes,
                   original_bytes, digest, level, verified_chunks=None, progress=None, dictionary=None,
                   manifest_dictionary=None):
    target = manifest_path(directory, name)
    existing = payload_exists(target)
    if existing:
        manifest = read_manifest(target, name)
        if (manifest['sha256'], manifest['original_bytes'], manifest['modified_ns']) != (digest, original_bytes, modified_ns):
            raise ValueError('Conflicting archive manifest; retained source')
    else:
        ensure_directory(target.parent)
        chunk_root = directory / 'request-chunks'
        ensure_directory(chunk_root)
    chunks = []
    actual = hashlib.sha256()
    total = 0
    verified_chunks = verified_chunks if verified_chunks is not None else {}
    prepared = ((chunk, None, None) for chunk in source_chunks) if existing else compressed_chunks(directory, source_chunks, level, dictionary)
    for chunk, reference, future in prepared:
        total += len(chunk)
        actual.update(chunk)
        if progress is not None:
            progress()
        if not existing:
            path = chunk_path(directory, reference['hash'])
            ensure_directory(path.parent)
            if not payload_exists(path):
                publish_bytes(path, future.result())
            stamp = payload_stamp(path)
            if verified_chunks.get(reference['hash']) != stamp:
                read_chunk(directory, reference)
                verified_chunks[reference['hash']] = stamp
            chunks.append(reference)
            if len(chunks) > MAX_CHUNKS:
                raise ValueError('Archive exceeds chunk reference limit; retained source')
    if (total, actual.hexdigest()) != (original_bytes, digest):
        raise ValueError('Archive differs from verified index; retained source')
    if not existing:
        manifest = validate_manifest({
            'schema_version': 1, 'name': name, 'modified_ns': modified_ns,
            'original_bytes': original_bytes, 'sha256': digest,
            'legacy_compressed_bytes': legacy_compressed_bytes, 'chunks': chunks,
        }, name)
        body = json.dumps(manifest, separators=(',', ':')).encode('utf-8')
        if len(body) > MANIFEST_MAX_BYTES:
            raise ValueError('Archive manifest exceeds limit')
        publish_bytes(target, zstd.compress(body, options={
            zstd.CompressionParameter.compression_level: level,
            zstd.CompressionParameter.checksum_flag: 1},
            zstd_dict=manifest_dictionary.as_digested_dict if manifest_dictionary is not None else None))
        # An independent compactor may have published a different manifest.
        manifest = read_manifest(target, name)
        if (manifest['sha256'], manifest['original_bytes'], manifest['modified_ns']) != (digest, original_bytes, modified_ns):
            raise ValueError('Conflicting archive publication; retained source')
    # Read back the actual published bytes before the caller removes a source.
    with open_record(directory, target, name) as source:
        verification = hashlib.sha256()
        while chunk := source.read(1024 * 1024):
            verification.update(chunk)
            if progress is not None:
                progress()
    if verification.hexdigest() != digest:
        raise ValueError('Archive publication failed verification')
    return manifest
