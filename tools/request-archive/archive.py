#!/usr/bin/env python3
"""Rebuildable SQLite index for immutable CLI Proxy API Zstandard request archives.

Python 3.14+. Payload files are authoritative; deleting this index loses no records.
"""
import argparse
from contextlib import contextmanager, ExitStack, closing
import fcntl
import zipfile
from datetime import datetime, timedelta, timezone
from compression import zstd
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import shutil
import tempfile
import sys
import time
import archive_dedup as dedup


def connect(directory):
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    db = sqlite3.connect(directory / 'request-index.sqlite3')
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=FULL')
    db.executescript('''
        CREATE TABLE IF NOT EXISTS records (
            name TEXT PRIMARY KEY,
            request_id TEXT NOT NULL,
            modified_ns INTEGER NOT NULL,
            compressed_bytes INTEGER NOT NULL,
            original_bytes INTEGER NOT NULL,
            sha256 TEXT NOT NULL,
            method TEXT,
            url TEXT,
            timestamp TEXT
        );
        CREATE INDEX IF NOT EXISTS by_request ON records(request_id);
        CREATE INDEX IF NOT EXISTS by_time ON records(modified_ns);
    ''')
    if 'container' not in {row[1] for row in db.execute('PRAGMA table_info(records)')}:
        db.execute("ALTER TABLE records ADD COLUMN container TEXT")
        db.commit()
    return db


@contextmanager
def open_member(path, name):
    with zipfile.ZipFile(path) as pack, pack.open(name) as member:
        yield member


@contextmanager
def archive_sources(directory):
    """List immutable members without keeping every pack's file descriptor open."""
    sources = {}
    for pack in sorted(directory.glob('requests-*.zip')):
        try:
            source = zipfile.ZipFile(pack)
        except FileNotFoundError:
            continue  # A concurrent optimizer may have published manifests.
        with source:
            for member in source.infolist():
                if Path(member.filename).name != member.filename or not member.filename.endswith('.log.zst'):
                    raise ValueError('Invalid pack member')
                stamp = int(member.comment)
                if member.filename in sources:
                    raise ValueError('Duplicate packed record')
                sources[member.filename] = (stamp, member.file_size, pack.name,
                    lambda pack=pack, name=member.filename: open_member(pack, name))
    for path in directory.glob('*.log.zst'):
        if path.name in sources:
            continue  # A published pack wins during crash recovery / source cleanup.
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue  # Another process may have just packed this record.
        sources[path.name] = (stat.st_mtime_ns, stat.st_size, None,
                              lambda path=path: path.open('rb'))
    for path, identity in dedup.payload_metadata_locations(directory / 'request-records', dedup.MANIFEST_SUFFIX):
        name = path.name.removesuffix(dedup.MANIFEST_SUFFIX)
        manifest = dedup.manifest_metadata(path, name, identity)
        container = str(path.relative_to(directory))
        sources[name] = (manifest['modified_ns'], manifest.get('legacy_compressed_bytes', 0),
                         container, lambda path=path, name=name: dedup.open_record(directory, path, name))
    yield sources


def is_deduplicated(container):
    return container is not None and container.startswith('request-records/')


@contextmanager
def open_source(container, opener):
    with opener() as source:
        if is_deduplicated(container):
            yield source
        else:
            with zstd.open(source, 'rb') as decoded:
                yield decoded


@contextmanager
def open_record(directory, name):
    # A concurrent compactor can remove a loose source after this listing.
    for attempt in range(2):
        with archive_sources(directory) as sources, ExitStack() as stack:
            if name not in sources:
                if attempt == 0:
                    continue
                raise FileNotFoundError(name)
            try:
                source = stack.enter_context(open_source(sources[name][2], sources[name][3]))
            except FileNotFoundError:
                if attempt == 0:
                    continue
                raise
            yield source
            return


def sync_directory(directory):
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def compact(db, directory, batch_size=100):
    """Publish verified packs durably before deleting any standalone archive.

    Only the background watcher calls this automatically. The lock serializes
    compactors; request writers never wait for it and publish new immutable files.
    """
    if batch_size < 100:
        raise ValueError('Compaction requires at least 100 completed requests')
    with (directory / '.request-compaction.lock').open('a+b') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        sync_directory(directory)
        # Finish cleanup after a crash between publication and source removal.
        with archive_sources(directory) as sources:
            for name, (_, _, container, opener) in sources.items():
                path = directory / name
                if container and path.exists():
                    with ExitStack() as stack:
                        packed = stack.enter_context(opener())
                        loose = stack.enter_context(path.open('rb'))
                        if is_deduplicated(container):
                            loose = stack.enter_context(zstd.open(loose, 'rb'))
                        while True:
                            a, b = packed.read(1024 * 1024), loose.read(1024 * 1024)
                            if a != b:
                                raise ValueError('Packed source differs; retained standalone archive')
                            if not a:
                                break
                    path.unlink()
        sync_directory(directory)
        index(db, directory)
        packed_count = 0
        while True:
            candidates = sorted(directory.glob('*.log.zst'))[:batch_size]
            if len(candidates) < batch_size:
                return packed_count
            fd, temporary = tempfile.mkstemp(prefix='.request-pack-', dir=directory)
            try:
                snapshots = {}
                with os.fdopen(fd, 'w+b') as output:
                    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_STORED,
                                         allowZip64=True) as pack:
                        for path in candidates:
                            stat = path.stat()
                            row = db.execute('SELECT modified_ns, compressed_bytes FROM records WHERE name=?',
                                             (path.name,)).fetchone()
                            if row != (stat.st_mtime_ns, stat.st_size):
                                raise ValueError('Unverified archive')
                            snapshots[path.name] = stat
                            member = zipfile.ZipInfo(path.name)
                            member.comment = str(stat.st_mtime_ns).encode('ascii')
                            member.external_attr = 0o100600 << 16
                            with path.open('rb') as source, pack.open(member, 'w', force_zip64=True) as target:
                                shutil.copyfileobj(source, target, 1024 * 1024)
                    output.flush()
                    os.fsync(output.fileno())
                # Validate every copied payload against the verified index before publication.
                with zipfile.ZipFile(temporary) as pack:
                    for member in pack.infolist():
                        digest = hashlib.sha256()
                        with pack.open(member) as compressed, zstd.open(compressed, 'rb') as source:
                            while chunk := source.read(1024 * 1024):
                                digest.update(chunk)
                        expected = db.execute('SELECT sha256 FROM records WHERE name=?',
                                              (member.filename,)).fetchone()[0]
                        if digest.hexdigest() != expected:
                            raise ValueError('Pack verification failed')
                destination = directory / ('requests-' + Path(temporary).name.removeprefix('.request-pack-') + '.zip')
                os.link(temporary, destination)
                sync_directory(directory)
                # The files are immutable, but refuse cleanup if an external process changed one.
                for path in candidates:
                    before, after = snapshots[path.name], path.stat()
                    if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                        raise ValueError('Source changed after packing; retained source')
                    path.unlink()
                sync_directory(directory)
                index(db, directory)
                packed_count += len(candidates)
            finally:
                if os.path.exists(temporary):
                    os.unlink(temporary)


def heartbeat_every(progress, interval=5):
    """Rate-limit a progress callback so long passes keep health reporting fresh."""
    last = time.monotonic()

    def beat(*args):
        nonlocal last
        if progress is not None and time.monotonic() - last >= interval:
            progress(*args)
            # Publication itself can be slow; wait a full interval after it.
            last = time.monotonic()
    return beat


def index(db, directory, progress=None):
    added = 0
    failures = []
    heartbeat = heartbeat_every(progress)
    with dedup.inventory_snapshot(), archive_sources(directory) as sources:
        for name, (modified_ns, compressed_bytes, container, opener) in sources.items():
            heartbeat(added)
            old = db.execute('SELECT modified_ns, compressed_bytes, container FROM records WHERE name=?',
                             (name,)).fetchone()
            if old == (modified_ns, compressed_bytes, container):
                continue
            digest = hashlib.sha256()
            size = 0
            header = b''
            # Read the complete record to validate the frame checksum and hash every byte.
            try:
                with open_source(container, opener) as source:
                    while chunk := source.read(1024 * 1024):
                        digest.update(chunk)
                        size += len(chunk)
                        if len(header) < 16384:
                            header += chunk[:16384 - len(header)]
            except Exception as error:
                # A damaged file must not prevent newer intact records being indexed.
                failures.append(error)
                continue
            fields = {}
            # Only parse request metadata before body data, never treat body text as metadata.
            info = header.split(b'=== HEADERS ===', 1)[0]
            for key in ('Method', 'URL', 'Timestamp'):
                match = re.search(rb'^' + key.encode() + rb': (.*)$', info, re.MULTILINE)
                fields[key] = match[1].decode('utf-8', 'replace') if match else None
            request_id = name.removesuffix('.zst').removesuffix('.log').rsplit('-', 1)[-1]
            with db:
                db.execute('INSERT OR REPLACE INTO records VALUES (?,?,?,?,?,?,?,?,?,?)',
                           (name, request_id, modified_ns, compressed_bytes, size,
                            digest.hexdigest(), fields['Method'], fields['URL'], fields['Timestamp'], container))
            added += 1
    if failures:
        raise failures[0]
    return added


def optimize(db, directory, chunk_bytes=dedup.DEFAULT_CHUNK_BYTES,
             string_bytes=dedup.DEFAULT_STRING_BYTES, level=dedup.DEFAULT_LEVEL,
             max_packs=None, max_loose=None, dry_run=False, progress=None,
             string_chunk_bytes=dedup.DEFAULT_STRING_CHUNK_BYTES, pack_tails=False,
             dictionary_path=None, manifest_dictionary_path=None):
    """Deduplicate legacy files after durable, byte-verified record publication.

    All sources remain authoritative until their replacement is independently
    verified. A pack is removed only after every member has a verified manifest.
    Published manifests win listing/read races and support restart recovery.
    """
    packs = sorted(directory.glob('requests-*.zip'))
    loose = sorted(directory.glob('*.log.zst'))
    if max_packs is not None:
        packs = packs[:max_packs]
    if max_loose is not None:
        loose = loose[:max_loose]
    if dry_run:
        return {'packs': len(packs), 'standalone': len(loose),
                'legacy_bytes': sum(path.stat().st_size for path in packs + loose)}
    if not 1 <= level <= 22:
        raise ValueError('Invalid archive compression level')
    if not 0 < chunk_bytes <= dedup.MAX_CHUNK_BYTES or not 0 < string_chunk_bytes <= dedup.MAX_CHUNK_BYTES or string_bytes < 1:
        raise ValueError('Invalid archive chunk settings')
    count = 0
    last_progress = time.monotonic()
    updates = []
    verified_chunks = {}
    with (directory / '.request-compaction.lock').open('a+b') as lock, dedup.inventory_snapshot():
        fcntl.flock(lock, fcntl.LOCK_EX)
        sync_directory(directory)
        dictionary = dedup.install_dictionary(directory, dictionary_path) if dictionary_path is not None else dedup.active_dictionary(directory)
        manifest_dictionary = dedup.install_dictionary(directory, manifest_dictionary_path, 'active-manifest-dictionary') if manifest_dictionary_path is not None else dedup.active_dictionary(directory, 'active-manifest-dictionary')

        def heartbeat():
            nonlocal last_progress
            now = time.monotonic()
            if progress is not None and now - last_progress >= 5:
                progress(count)
                # Inventory scans can exceed the interval on a large archive.
                # Wait a full interval after publication before scanning again.
                last_progress = time.monotonic()

        def convert(name, stamp, compressed_bytes, source):
            nonlocal count
            expected = db.execute('SELECT modified_ns, original_bytes, sha256 FROM records WHERE name=?',
                                  (name,)).fetchone()
            if expected is None:
                raise ValueError('Unverified archive; run index first')
            if expected[0] != stamp:
                raise ValueError('Archive differs from verified index; retained source')
            with zstd.open(source, 'rb') as decoded:
                if expected[1] > dedup.MAX_SEMANTIC_RECORD_BYTES:
                    dedup.publish_stream_record(directory, name, decoded, stamp, compressed_bytes,
                                                expected[1], expected[2], chunk_bytes, level,
                                                verified_chunks, heartbeat, dictionary, manifest_dictionary)
                else:
                    data = decoded.read(dedup.MAX_SEMANTIC_RECORD_BYTES + 1)
                    if (len(data), hashlib.sha256(data).hexdigest()) != expected[1:]:
                        raise ValueError('Archive differs from verified index; retained source')
                    dedup.publish_record(directory, name, data, stamp, compressed_bytes,
                                         chunk_bytes, string_bytes, level, verified_chunks,
                                         string_chunk_bytes, heartbeat, dictionary, manifest_dictionary)
            updates.append((str(dedup.manifest_path(directory, name).relative_to(directory)), name))
            count += 1

        def update_index():
            # Publication already verified every byte against the existing row.
            # Avoid decompressing the same records again on every migrated pack.
            with db:
                db.executemany('UPDATE records SET container=? WHERE name=?', updates)
            updates.clear()

        for path in packs:
            if not path.exists():
                continue
            before = path.stat()
            with zipfile.ZipFile(path) as pack:
                for member in pack.infolist():
                    if Path(member.filename).name != member.filename or not member.filename.endswith('.log.zst'):
                        raise ValueError('Invalid pack member')
                    with pack.open(member) as source:
                        convert(member.filename, int(member.comment), member.file_size, source)
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError('Archive pack changed; retained source')
            path.unlink()
            sync_directory(directory)
            update_index()
            if progress is not None:
                progress(count)
                last_progress = time.monotonic()
        for path in loose:
            if not path.exists():
                continue
            # The recorder publishes continuously, so records can land after this
            # cycle's index pass. Leave them for the next cycle instead of failing.
            if db.execute('SELECT 1 FROM records WHERE name=?', (path.name,)).fetchone() is None:
                continue
            before = path.stat()
            with path.open('rb') as source:
                convert(path.name, before.st_mtime_ns, before.st_size, source)
            after = path.stat()
            if (before.st_ino, before.st_size, before.st_mtime_ns) != (after.st_ino, after.st_size, after.st_mtime_ns):
                raise ValueError('Standalone archive changed; retained source')
            path.unlink()
            sync_directory(directory)
        if loose:
            update_index()
        if progress is not None:
            progress(count)
            last_progress = time.monotonic()
        packed = dedup.compact_storage(directory, all_tails=pack_tails,
                                        progress=lambda _: heartbeat())
    return {'optimized': count, **packed}


def publish_stats(db, directory, error=None, now=None):
    """Publish bounded metadata atomically; dashboard requests never scan payloads."""
    now = now or datetime.now(timezone.utc)
    totals = db.execute("""SELECT count(*), coalesce(sum(compressed_bytes),0),
        coalesce(sum(original_bytes),0), min(modified_ns), max(modified_ns) FROM records""").fetchone()
    first_day = (now - timedelta(days=6)).date()
    # modified_ns is the completed record's preserved write time, grouped in UTC.
    rows = db.execute("""SELECT date(modified_ns/1000000000, 'unixepoch'), count(*)
        FROM records WHERE modified_ns >= ? GROUP BY 1""",
        (int(datetime.combine(first_day, datetime.min.time(), timezone.utc).timestamp() * 1e9),)).fetchall()
    daily = dict(rows)
    manifest_root = directory / 'request-records'
    chunk_root = directory / 'request-chunks'
    manifests = dedup.payload_paths(manifest_root, dedup.MANIFEST_SUFFIX)
    chunks = []
    for shard in chunk_root.glob('*'):
        if shard.is_dir() and shard.name != 'dictionaries':
            chunks.extend(dedup.payload_paths(shard, '.zst'))
    storage = list(manifest_root.glob('*' + dedup.MANIFEST_SUFFIX)) + list(manifest_root.glob('records-*.zip'))
    storage += list(chunk_root.glob('*/*.zst')) + list(chunk_root.glob('*/chunks-*.zip'))
    storage += list((chunk_root / 'dictionaries').glob('*.zdict'))
    for pointer_name in dedup.DICTIONARY_POINTERS:
        pointer = chunk_root / 'dictionaries' / pointer_name
        if pointer.is_file():
            storage.append(pointer)
    files = list(directory.glob('*.log.zst')) + list(directory.glob('requests-*.zip')) + storage
    with archive_sources(directory) as sources:
        sources = dict(sources)
    disk = shutil.disk_usage(directory)
    index_bytes = sum(p.stat().st_size for p in directory.glob('request-index.sqlite3*') if p.is_file())
    latest = lambda value: datetime.fromtimestamp(value / 1e9, timezone.utc).isoformat() if value else None
    stats = {
        'schema_version': 1,
        'updated_at': now.isoformat(),
        'state': 'error' if error else 'healthy',
        # Error type only: filenames or exception text can contain private data.
        'error_kind': type(error).__name__ if error else None,
        'records': totals[0],
        'archive_files': len(sources),
        'storage_files': len(files),
        'pack_files': sum(p.suffix == '.zip' for p in files),
        'deduplicated_records': len(manifests),
        'shared_chunks': len(chunks),
        'archived_bytes': sum(p.stat().st_size for p in files),
        'original_bytes': totals[2],
        'index_bytes': index_bytes,
        'oldest_at': latest(totals[3]),
        'latest_at': latest(totals[4]),
        'disk_free_bytes': disk.free,
        'disk_total_bytes': disk.total,
        'pending_plaintext_files': sum(1 for p in directory.glob('*.log')
            if p.name != 'main.log' and re.search(r'-\d{4}-\d{2}-\d{2}T\d{6}.*\.log$', p.name)),
        'daily': [{'date': (first_day + timedelta(days=i)).isoformat(),
                   'records': daily.get((first_day + timedelta(days=i)).isoformat(), 0)}
                  for i in range(7)],
    }
    fd, temporary = tempfile.mkstemp(prefix='.request-archive-stats-', dir=directory)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(stats, output, separators=(',', ':'))
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, directory / 'request-archive-stats.json')
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
    return stats


SLOW_CYCLE_SECONDS = 30


def log(message):
    # Timestamps and error types only: paths or exception text can contain private data.
    print(f'{datetime.now(timezone.utc).isoformat(timespec="seconds")} {message}', file=sys.stderr, flush=True)


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True, type=Path)
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('index')
    sub.add_parser('watch')
    optimization = sub.add_parser('optimize')
    optimization.add_argument('--dry-run', action='store_true')
    optimization.add_argument('--chunk-size', type=int, default=dedup.DEFAULT_CHUNK_BYTES,
                              help='Maximum uncompressed chunk bytes')
    optimization.add_argument('--string-threshold', type=int, default=dedup.DEFAULT_STRING_BYTES,
                              help='Minimum raw JSON string bytes to split independently')
    optimization.add_argument('--string-chunk-size', type=int, default=dedup.DEFAULT_STRING_CHUNK_BYTES,
                              help='Maximum bytes per chunk inside a long raw JSON string')
    optimization.add_argument('--level', type=int, default=dedup.DEFAULT_LEVEL)
    optimization.add_argument('--dictionary', type=Path,
                              help='Install and activate an immutable trained Zstandard dictionary')
    optimization.add_argument('--manifest-dictionary', type=Path,
                              help='Install and activate a separate trained dictionary for manifests')
    search = sub.add_parser('search')
    search.add_argument('--request-id')
    search.add_argument('--url')
    search.add_argument('--limit', type=int, default=50)
    export = sub.add_parser('export')
    export.add_argument('name', help='Archive basename returned by search')
    export.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    directory = args.directory.resolve()
    if args.command == 'export':
        if Path(args.name).name != args.name or not args.name.endswith('.log.zst'):
            parser.error('Expected an archive basename ending in .log.zst')
        # Exclusive creation prevents accidentally replacing an existing file.
        with open_record(directory, args.name) as source, args.output.open('xb') as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)
            target.flush()
            os.fsync(target.fileno())
        return
    with closing(connect(directory)) as db:
        if args.command == 'watch':
            report = lambda _: publish_stats(db, directory)
            while True:
                phases = {}
                started = time.monotonic()
                try:
                    index(db, directory, progress=report)
                    phases['index'] = time.monotonic() - started
                    optimize(db, directory, max_packs=1, max_loose=100, progress=report)
                    phases['optimize'] = time.monotonic() - started - phases['index']
                    publish_stats(db, directory)
                except Exception as error:
                    log(f'Archive indexing failed: {type(error).__name__}; will retry')
                    try:
                        publish_stats(db, directory, error=error)
                    except Exception as report_error:
                        log(f'Archive health publication failed: {type(report_error).__name__}')
                elapsed = time.monotonic() - started
                if elapsed >= SLOW_CYCLE_SECONDS:
                    timings = ' '.join(f'{phase}={seconds:.1f}s' for phase, seconds in phases.items())
                    log(f'Archive cycle slow: total={elapsed:.1f}s {timings}'.rstrip())
                time.sleep(10)
        elif args.command == 'index':
            added = index(db, directory)
            publish_stats(db, directory)
            print(json.dumps({'indexed': added}))
        elif args.command == 'optimize':
            if not args.dry_run:
                index(db, directory)
            result = optimize(db, directory, args.chunk_size, args.string_threshold, args.level,
                              dry_run=args.dry_run,
                              progress=lambda _: publish_stats(db, directory),
                              string_chunk_bytes=args.string_chunk_size, pack_tails=True,
                              dictionary_path=args.dictionary,
                              manifest_dictionary_path=args.manifest_dictionary)
            if not args.dry_run:
                publish_stats(db, directory)
            print(json.dumps(result))
        else:
            index(db, directory)
            db.row_factory = sqlite3.Row
            rows = db.execute('''SELECT * FROM records
                WHERE (? IS NULL OR request_id = ?) AND (? IS NULL OR url LIKE ?)
                ORDER BY modified_ns DESC LIMIT ?''',
                (args.request_id, args.request_id, args.url,
                 '%' + args.url + '%' if args.url else None, max(1, args.limit)))
            print(json.dumps([dict(row) for row in rows], indent=2))


if __name__ == '__main__':
    main()
