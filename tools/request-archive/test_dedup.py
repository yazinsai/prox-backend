import importlib.util
from pathlib import Path
from contextlib import closing
from compression import zstd
import base64
import hashlib
import json
import random
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('archive', Path(__file__).with_name('archive.py'))
archive = importlib.util.module_from_spec(spec)
spec.loader.exec_module(archive)
dedup = archive.dedup


class DedupArchiveTests(unittest.TestCase):
    def make_record(self, directory, number, payload):
        name = f'v1-responses-2026-11-05T150000-{number:08d}.log.zst'
        body = (b'=== REQUEST INFO ===\nURL: /v1/responses\nMethod: POST\n'
                b'Timestamp: 2026-11-05T15:00:00Z\n=== HEADERS ===\n\n' + payload)
        with zstd.open(directory / name, 'wb') as output:
            output.write(body)
        return name, body

    def test_lossless_shared_images_rebuild_and_physical_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            image = b'data:image/png;base64,' + base64.b64encode(random.Random(7).randbytes(100000))
            records = dict(self.make_record(directory, i,
                b'=== REQUEST BODY ===\n{"prefix":"' + b'x' * (i * 100) + b'",'
                b'"image_url":"' + image + b'","suffix":' + str(i).encode() + b'}\x00\xff\n')
                for i in range(3))
            stamps = {name: (directory / name).stat().st_mtime_ns for name in records}
            legacy_bytes = sum((directory / name).stat().st_size for name in records)
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                preview = archive.optimize(db, directory, dry_run=True)
                self.assertEqual(preview, {'packs': 0, 'standalone': 3, 'legacy_bytes': legacy_bytes})
                self.assertFalse((directory / 'request-records').exists())
                self.assertEqual(archive.optimize(db, directory, chunk_bytes=32768)['optimized'], 3)
                self.assertEqual(list(directory.glob('*.log.zst')), [])
                references = []
                for name, body in records.items():
                    with archive.open_record(directory, name) as source:
                        self.assertEqual(source.read(), body)
                    manifest = dedup.read_manifest(dedup.manifest_path(directory, name), name)
                    references.append({chunk['hash'] for chunk in manifest['chunks']})
                self.assertTrue(set.intersection(*references))
                unique = list((directory / 'request-chunks').glob('*/*.zst'))
                self.assertLess(len(unique), sum(len(value) for value in references))
                stats = archive.publish_stats(db, directory)
                self.assertEqual(stats['records'], 3)
                self.assertEqual(stats['archive_files'], 3)
                self.assertEqual(stats['deduplicated_records'], 3)
                self.assertEqual(stats['shared_chunks'], len(unique))
                physical = unique + list((directory / 'request-records').glob('*.json.zst'))
                self.assertEqual(stats['archived_bytes'], sum(p.stat().st_size for p in physical))
                self.assertLess(stats['archived_bytes'], legacy_bytes / 2)
                db.execute('DELETE FROM records')
                db.commit()
                self.assertEqual(archive.index(db, directory), 3)
                for name, body in records.items():
                    row = db.execute('SELECT modified_ns,original_bytes,sha256,url FROM records WHERE name=?', (name,)).fetchone()
                    self.assertEqual(row, (stamps[name], len(body), hashlib.sha256(body).hexdigest(), '/v1/responses'))

    def test_pack_publication_crash_recovery_keeps_every_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            records = dict(self.make_record(directory, i, b'private fixture bytes\x00\xff' * 100) for i in range(100))
            with closing(archive.connect(directory)) as db:
                archive.compact(db, directory)
                original_unlink = Path.unlink
                def fail_pack_removal(path, *args, **kwargs):
                    if path.name.startswith('requests-') and path.suffix == '.zip':
                        raise OSError('simulated crash after manifests')
                    return original_unlink(path, *args, **kwargs)
                with patch.object(Path, 'unlink', fail_pack_removal):
                    with self.assertRaises(OSError):
                        archive.optimize(db, directory)
                self.assertEqual(len(list(directory.glob('requests-*.zip'))), 1)
                self.assertEqual(len(list((directory / 'request-records').glob('*.json.zst'))), 100)
                for name, body in records.items():
                    with archive.open_record(directory, name) as source:
                        self.assertEqual(source.read(), body)
                chunks = len(list((directory / 'request-chunks').glob('*/*.zst')))
                self.assertEqual(archive.optimize(db, directory)['optimized'], 100)
                self.assertEqual(list(directory.glob('requests-*.zip')), [])
                self.assertEqual(len(list((directory / 'request-chunks').glob('*/*.zst'))), chunks)
                self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0], 100)

    def test_corrupt_shared_chunk_is_rejected_on_read_and_rebuild(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name, _ = self.make_record(directory, 1, b'record payload')
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                archive.optimize(db, directory)
                manifest = dedup.read_manifest(dedup.manifest_path(directory, name), name)
                chunk = manifest['chunks'][0]
                with zstd.open(dedup.chunk_path(directory, chunk['hash']), 'wb') as target:
                    target.write(b'x' * chunk['size'])
                with self.assertRaises(ValueError):
                    with archive.open_record(directory, name) as source:
                        source.read()
                db.execute('DELETE FROM records')
                db.commit()
                self.make_record(directory, 2, b'intact unrelated exchange')
                with self.assertRaises(ValueError):
                    archive.index(db, directory)
                self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0], 1)

    def test_legacy_compaction_recovers_deduplicated_standalone_source(self):
        for conflicting in (False, True):
            with self.subTest(conflicting=conflicting), tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                name, body = self.make_record(directory, 1, b'publication before cleanup')
                path = directory / name
                dedup.publish_record(directory, name, body, path.stat().st_mtime_ns,
                                     path.stat().st_size)
                if conflicting:
                    with zstd.open(path, 'wb') as output:
                        output.write(body + b'changed source')
                with closing(archive.connect(directory)) as db:
                    if conflicting:
                        with self.assertRaises(ValueError):
                            archive.compact(db, directory)
                        self.assertTrue(path.exists())
                    else:
                        self.assertEqual(archive.compact(db, directory), 0)
                        self.assertFalse(path.exists())
                        with archive.open_record(directory, name) as source:
                            self.assertEqual(source.read(), body)

    def test_slow_stats_publication_does_not_starve_optimization(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.make_record(directory, 1, b'"' + b'a' * 100000 + b'"')
            clock = [0]
            reports = []
            original_split = dedup.split_chunks

            def split(*args, **kwargs):
                clock[0] += 10
                yield from original_split(*args, **kwargs)

            def report(count):
                reports.append(count)
                clock[0] += 10  # A large inventory can take longer than the heartbeat interval.

            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                with patch.object(archive.time, 'monotonic', lambda: clock[0]), \
                     patch.object(dedup, 'split_chunks', split):
                    result = archive.optimize(db, directory, chunk_bytes=1024,
                                              string_chunk_bytes=1024, progress=report)
                self.assertEqual(result['optimized'], 1)
                self.assertEqual(reports, [0, 1])

    def test_failed_publication_and_conflicts_never_remove_original(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name, body = self.make_record(directory, 1, b'original\n')
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                with patch.object(dedup.os, 'link', side_effect=OSError('publication failed')):
                    with self.assertRaises(OSError):
                        archive.optimize(db, directory)
                self.assertTrue((directory / name).exists())
                stamp = (directory / name).stat().st_mtime_ns
                dedup.publish_record(directory, name, body + b'conflicting', stamp, 0)
                with self.assertRaises(ValueError):
                    archive.optimize(db, directory)
                with zstd.open(directory / name, 'rb') as source:
                    self.assertEqual(source.read(), body)

    def test_chunk_boundaries_retain_arbitrary_binary_and_escaped_strings(self):
        inputs = [b'', bytes(range(256)) * 900, b'"' + b'escaped\\"text\\\\' * 4000 + b'"',
                  b'partial "unterminated' + b'x' * 90000, b'{"a":"short","b":"' + b'x' * 20000 + b'"}']
        for body in inputs:
            with self.subTest(size=len(body)):
                chunks = list(dedup.split_chunks(body, chunk_bytes=8192, string_chunk_bytes=8192))
                self.assertEqual(b''.join(chunks), body)
                self.assertTrue(all(0 < len(chunk) <= 8192 for chunk in chunks))

    def test_manifest_bounds_digests_and_symlinks(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name = 'v1-responses-2026-11-05T150000-00000001.log.zst'
            dedup.publish_record(directory, name, b'payload', 1, 7)
            path = dedup.manifest_path(directory, name)
            manifest = dedup.read_manifest(path, name)
            for changes in ({'name': '../escape.log.zst'}, {'modified_ns': True},
                            {'schema_version': True}, {'modified_ns': dedup.MAX_COUNTER + 1},
                            {'sha256': '../escape'}, {'original_bytes': 123},
                            {'chunks': [{'hash': manifest['sha256'], 'size': dedup.MAX_CHUNK_BYTES + 1}]}):
                with self.subTest(changes=changes):
                    with self.assertRaises(ValueError):
                        dedup.validate_manifest(manifest | changes, name)
            chunk = manifest['chunks'][0]
            chunk_file = dedup.chunk_path(directory, chunk['hash'])
            other = directory / 'external.zst'
            chunk_file.rename(other)
            chunk_file.symlink_to(other)
            with self.assertRaises(ValueError):
                dedup.read_chunk(directory, chunk)
            path.unlink()
            with zstd.open(path, 'wb') as target:
                target.write(b' ' * (dedup.MANIFEST_MAX_BYTES + 1))
            with self.assertRaises(ValueError):
                dedup.read_manifest(path)

    def test_packed_manifests_and_chunks_export_rebuild_and_stats(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            records = dict(self.make_record(directory, i, b'packed payload\x00\xff' + bytes([i]) * 2000) for i in range(5))
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                result = archive.optimize(db, directory, pack_tails=True)
                self.assertEqual(result['packed_manifests'], 5)
                self.assertGreater(result['packed_chunks'], 0)
                self.assertEqual(list((directory / 'request-records').glob('*.json.zst')), [])
                self.assertEqual(list((directory / 'request-chunks').glob('*/*.zst')), [])
                for name, body in records.items():
                    with archive.open_record(directory, name) as source:
                        self.assertEqual(source.read(), body)
                db.execute('DELETE FROM records')
                db.commit()
                self.assertEqual(archive.index(db, directory), 5)
                stats = archive.publish_stats(db, directory)
                self.assertEqual(stats['records'], 5)
                self.assertEqual(stats['deduplicated_records'], 5)
                packs = list((directory / 'request-records').glob('*.zip')) + list((directory / 'request-chunks').glob('*/*.zip'))
                self.assertEqual(stats['archived_bytes'], sum(path.stat().st_size for path in packs))

    def test_manifest_pack_crash_cleanup_checks_duplicate_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name, body = self.make_record(directory, 1, b'crash fixture')
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                archive.optimize(db, directory)
                original_unlink = Path.unlink
                def fail_manifest_removal(path, *args, **kwargs):
                    if path.name.endswith('.json.zst'):
                        raise OSError('simulated manifest cleanup crash')
                    return original_unlink(path, *args, **kwargs)
                with patch.object(Path, 'unlink', fail_manifest_removal):
                    with self.assertRaises(OSError):
                        dedup.compact_storage(directory, all_tails=True)
                path = dedup.manifest_path(directory, name)
                self.assertTrue(path.exists())
                self.assertEqual(len(list(path.parent.glob('records-*.zip'))), 1)
                with archive.open_record(directory, name) as source:
                    self.assertEqual(source.read(), body)
                original = path.read_bytes()
                path.write_bytes(b'conflicting source')
                with self.assertRaises(ValueError):
                    dedup.compact_storage(directory, all_tails=True)
                self.assertTrue(path.exists())
                path.write_bytes(original)
                dedup.compact_storage(directory, all_tails=True)
                self.assertFalse(path.exists())
                with archive.open_record(directory, name) as source:
                    self.assertEqual(source.read(), body)

    def test_invalid_packed_member_names_and_encoding_are_rejected(self):
        import zipfile
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            shard = directory / 'request-chunks' / 'ab'
            shard.mkdir(parents=True)
            path = shard / 'chunks-invalid.zip'
            for name, encoding in (('../escape.zst', zipfile.ZIP_STORED),
                                   ('cd' + '0' * 62 + '.zst', zipfile.ZIP_STORED),
                                   ('ab' + '0' * 62 + '.zst', zipfile.ZIP_DEFLATED)):
                with self.subTest(name=name, encoding=encoding):
                    with zipfile.ZipFile(path, 'w', compression=encoding) as pack:
                        pack.writestr(name, b'invalid archive bytes')
                    with self.assertRaises(ValueError):
                        dedup.packed_index(shard)

    def test_large_record_streaming_fallback_and_heartbeat(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name, body = self.make_record(directory, 1, random.Random(8).randbytes(16000))
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                progress = []
                ticks = iter(range(0, 10000, 10))
                with patch.object(dedup, 'MAX_SEMANTIC_RECORD_BYTES', 1024), \
                        patch.object(archive.time, 'monotonic', side_effect=lambda: next(ticks)):
                    archive.optimize(db, directory, chunk_bytes=4096, progress=progress.append)
                self.assertGreater(len(progress), 1)
                manifest = dedup.read_manifest(dedup.manifest_path(directory, name), name)
                self.assertTrue(all(chunk['size'] <= 4096 for chunk in manifest['chunks']))
                with archive.open_record(directory, name) as source:
                    self.assertEqual(source.read(), body)

    def test_scalar_metadata_cache_replacement_packing_and_connection_change(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / 'one'
            other = Path(tmp) / 'two'
            directory.mkdir()
            other.mkdir()
            name = 'v1-responses-2026-11-05T150000-00000001.log.zst'
            dedup.publish_record(directory, name, b'fixture', 1, 7)
            dedup.publish_record(other, name, b'other fixture', 8, 13)
            path = dedup.manifest_path(directory, name)
            with patch.object(dedup, 'read_manifest', wraps=dedup.read_manifest) as reader:
                first = dedup.manifest_metadata(path, name)
                again = dedup.manifest_metadata(path, name)
                self.assertEqual(first, again)
                self.assertEqual(reader.call_count, 1)
                self.assertNotIn('chunks', first)
                self.assertEqual(set(first), {'name', 'modified_ns', 'original_bytes', 'sha256', 'legacy_compressed_bytes'})
                manifest = reader(path, name)
                manifest['modified_ns'] = 2
                replacement = path.with_name('replacement.zst')
                with zstd.open(replacement, 'wb') as target:
                    target.write(json.dumps(manifest).encode())
                replacement.replace(path)
                self.assertEqual(dedup.manifest_metadata(path, name)['modified_ns'], 2)
                before = reader.call_count
                dedup.compact_storage(directory, all_tails=True)
                self.assertEqual(dedup.manifest_metadata(path, name)['modified_ns'], 2)
                self.assertGreater(reader.call_count, before)
                self.assertEqual(dedup.manifest_metadata(dedup.manifest_path(other, name), name)['modified_ns'], 8)
                self.assertEqual(len(dedup._MANIFEST_METADATA), 1)
                self.assertEqual(dedup._MANIFEST_METADATA_DIRECTORY, other / 'request-records')
                self.assertEqual(dedup.manifest_metadata(path, name)['modified_ns'], 2)
                self.assertEqual(dedup._MANIFEST_METADATA_DIRECTORY, directory / 'request-records')

    def test_dictionary_roundtrip_packed_payloads_missing_and_corrupt_dictionary(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            samples = [(b'fixture header compression repeated history ' + str(i).encode() + b'\x00') * 20
                       for i in range(100)]
            dictionary = zstd.train_dict(samples, 2048)
            source_path = directory / 'source.zdict'
            source_path.write_bytes(dictionary.dict_content)
            legacy_name, legacy_body = self.make_record(directory, 1, b'legacy dictionary-free bytes')
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                archive.optimize(db, directory, pack_tails=True)
                name, body = self.make_record(directory, 2, samples[5] * 5)
                archive.index(db, directory)
                archive.optimize(db, directory, dictionary_path=source_path,
                                 manifest_dictionary_path=source_path, pack_tails=True)
                self.assertEqual(dedup.active_dictionary(directory).dict_id, dictionary.dict_id)
                path = dedup.manifest_path(directory, name)
                with dedup.open_payload(path) as compressed:
                    self.assertEqual(zstd.get_frame_info(compressed.read(18)).dictionary_id, dictionary.dict_id)
                for logical_name, expected in ((legacy_name, legacy_body), (name, body)):
                    with archive.open_record(directory, logical_name) as reader:
                        self.assertEqual(reader.read(), expected)
                stats = archive.publish_stats(db, directory)
                stored_dictionary = dedup.dictionary_root(directory) / (str(dictionary.dict_id) + '.zdict')
                self.assertTrue(stored_dictionary.exists())
                dictionary_bytes = stored_dictionary.stat().st_size + sum(
                    (stored_dictionary.parent / pointer).stat().st_size for pointer in dedup.DICTIONARY_POINTERS)
                physical = list((directory / 'request-records').glob('*.zip')) + list((directory / 'request-chunks').glob('*/chunks-*.zip'))
                self.assertEqual(stats['archived_bytes'], sum(path.stat().st_size for path in physical) + dictionary_bytes)
                stored_dictionary.unlink()
                with self.assertRaises(FileNotFoundError):
                    with archive.open_record(directory, name) as reader:
                        reader.read()
                stored_dictionary.write_bytes(b'corrupt dictionary')
                with self.assertRaises(ValueError):
                    with archive.open_record(directory, name) as reader:
                        reader.read()
                with archive.open_record(directory, legacy_name) as reader:
                    self.assertEqual(reader.read(), legacy_body)

    def test_compression_prefetch_failure_retains_source_and_deduplicates_pending_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name, body = self.make_record(directory, 1, b'x' * 20000)
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                with patch.object(dedup.zstd, 'compress', side_effect=RuntimeError('worker failure')):
                    with self.assertRaises(RuntimeError):
                        archive.optimize(db, directory)
                self.assertTrue((directory / name).exists())
                self.assertFalse(dedup.manifest_path(directory, name).exists())
            # The repeated chunk enters the bounded queue several times before
            # its first publication. Only one worker compresses that digest.
            expected = hashlib.sha256(b'identical shared chunk').hexdigest()
            with patch.object(dedup.zstd, 'compress', wraps=zstd.compress) as compressor:
                iterator = dedup.compressed_chunks(directory, [b'identical shared chunk'] * 8, 3, None)
                values = list(iterator)
                values[0][2].result()
                self.assertEqual(compressor.call_count, 1)
                self.assertEqual({value[1]['hash'] for value in values}, {expected})
                self.assertEqual(len({id(value[2]) for value in values}), 1)
                self.assertLessEqual(len(values), dedup.COMPRESSION_PREFETCH)

    def test_independent_chunk_and_manifest_dictionaries(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            samples = [(b'chunk history payload ' + str(i).encode()) * 100 for i in range(100)]
            manifest_samples = [json.dumps({'schema_version': 1, 'name': str(i),
                               'chunks': [{'hash': hashlib.sha256(str(i).encode()).hexdigest(), 'size': i + 1}]}).encode() * 10
                               for i in range(100)]
            dictionaries = [zstd.train_dict(samples, 2048), zstd.train_dict(manifest_samples, 2048)]
            self.assertNotEqual(dictionaries[0].dict_id, dictionaries[1].dict_id)
            paths = [directory / 'chunks.zdict', directory / 'manifests.zdict']
            for path, dictionary in zip(paths, dictionaries):
                path.write_bytes(dictionary.dict_content)
            name, body = self.make_record(directory, 1, samples[3])
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                archive.optimize(db, directory, dictionary_path=paths[0],
                                 manifest_dictionary_path=paths[1], pack_tails=True)
                manifest_path = dedup.manifest_path(directory, name)
                with dedup.open_payload(manifest_path) as source:
                    self.assertEqual(zstd.get_frame_info(source.read(18)).dictionary_id, dictionaries[1].dict_id)
                manifest = dedup.read_manifest(manifest_path, name)
                for chunk in manifest['chunks']:
                    with dedup.open_payload(dedup.chunk_path(directory, chunk['hash'])) as source:
                        self.assertEqual(zstd.get_frame_info(source.read(18)).dictionary_id, dictionaries[0].dict_id)
                with archive.open_record(directory, name) as source:
                    self.assertEqual(source.read(), body)
                root = dedup.dictionary_root(directory)
                chunk_dictionary = root / (str(dictionaries[0].dict_id) + '.zdict')
                chunk_dictionary.unlink()
                self.assertEqual(dedup.read_manifest(manifest_path, name)['sha256'], hashlib.sha256(body).hexdigest())
                with self.assertRaises(FileNotFoundError):
                    with archive.open_record(directory, name) as source:
                        source.read()
                chunk_dictionary.write_bytes(dictionaries[0].dict_content)
                (root / (str(dictionaries[1].dict_id) + '.zdict')).unlink()
                with self.assertRaises(FileNotFoundError):
                    dedup.read_manifest(manifest_path, name)

    def test_pack_inventory_reads_scan_once_and_optimizer_publication_refreshes(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name = 'v1-responses-2026-11-05T150000-00000001.log.zst'
            dedup.publish_record(directory, name, b'original packed chunk', 1, 8)
            dedup.compact_storage(directory, all_tails=True)
            manifest = dedup.read_manifest(dedup.manifest_path(directory, name), name)
            reference = manifest['chunks'][0]
            shard = dedup.chunk_path(directory, reference['hash']).parent
            original_glob = Path.glob
            scans = []
            def counted_glob(path, pattern):
                if path == shard and pattern == 'chunks-*.zip':
                    scans.append(path)
                return original_glob(path, pattern)
            dedup._PACK_INDEX.clear()
            dedup._PACK_GENERATIONS.clear()
            with patch.object(Path, 'glob', counted_glob):
                for _ in range(10):
                    self.assertEqual(dedup.read_chunk(directory, reference), b'original packed chunk')
                self.assertEqual(len(scans), 1)
                with dedup.inventory_snapshot():
                    self.assertEqual(dedup.read_chunk(directory, reference), b'original packed chunk')
                    before = len(scans)
                    # Creating a loose payload changes directory generation but
                    # must not rescan packs inside the optimizer's locked scope.
                    for i in range(100000):
                        body = b'new loose chunk ' + str(i).encode()
                        digest = hashlib.sha256(body).hexdigest()
                        if digest[:2] == shard.name:
                            break
                    new_reference = {'hash': digest, 'size': len(body)}
                    path = dedup.chunk_path(directory, digest)
                    path.write_bytes(zstd.compress(body))
                    for _ in range(10):
                        self.assertEqual(dedup.read_chunk(directory, new_reference), body)
                    self.assertEqual(len(scans), before)
                    dedup.compact_storage(directory, all_tails=True)
                    self.assertFalse(path.exists())
                    self.assertEqual(dedup.read_chunk(directory, new_reference), body)
                    self.assertGreater(len(scans), before)
                (shard / 'generation-marker').write_bytes(b'changed directory')
                before = len(scans)
                for _ in range(10):
                    self.assertEqual(dedup.read_chunk(directory, reference), b'original packed chunk')
                self.assertEqual(len(scans), before + 1)

    def test_optimize_defers_records_published_after_index(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            first, first_body = self.make_record(directory, 1, b'indexed\n')
            with closing(archive.connect(directory)) as db:
                archive.index(db, directory)
                # The recorder can publish between the index pass and the optimizer.
                late, late_body = self.make_record(directory, 2, b'arrived after index\n')
                self.assertEqual(archive.optimize(db, directory)['optimized'], 1)
                self.assertFalse((directory / first).exists())
                self.assertTrue((directory / late).exists())
                archive.index(db, directory)
                self.assertEqual(archive.optimize(db, directory)['optimized'], 1)
                self.assertFalse((directory / late).exists())
                for name, body in ((first, first_body), (late, late_body)):
                    with archive.open_record(directory, name) as source:
                        self.assertEqual(source.read(), body)
                stats = archive.publish_stats(db, directory)
                self.assertEqual((stats['state'], stats['records'], stats['archive_files']), ('healthy', 2, 2))

    def test_inventory_snapshot_reuses_unchanged_packs_and_sees_new_ones(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            name = 'v1-responses-2026-11-05T150000-00000001.log.zst'
            dedup.publish_record(directory, name, b'first packed chunk', 1, 8)
            dedup.compact_storage(directory, all_tails=True)
            reference = dedup.read_manifest(dedup.manifest_path(directory, name), name)['chunks'][0]
            shard = dedup.chunk_path(directory, reference['hash']).parent
            dedup._PACK_INDEX.clear()
            dedup._PACK_GENERATIONS.clear()
            dedup.packed_index(shard)
            opened = []
            original = dedup.zipfile.ZipFile
            def counted(file, *args, **kwargs):
                opened.append(file)
                return original(file, *args, **kwargs)
            with patch.object(dedup.zipfile, 'ZipFile', counted):
                for _ in range(3):
                    with dedup.inventory_snapshot():
                        dedup.packed_index(shard)
                self.assertEqual(opened, [])
            body = b'second packed chunk'
            digest = hashlib.sha256(body).hexdigest()
            loose = dedup.chunk_path(directory, digest)
            loose.parent.mkdir(parents=True, exist_ok=True)
            loose.write_bytes(dedup.zstd.compress(body))
            # Publishing a new immutable pack must be visible to the next snapshot.
            dedup.pack_payloads(loose.parent, 'chunks-', '.zst', 1,
                                lambda path: dedup.validate_chunk_file(directory, path), all_tails=True)
            self.assertFalse(loose.exists())
            with dedup.inventory_snapshot():
                self.assertIn(loose.name, dedup.packed_index(loose.parent))
                self.assertEqual(dedup.read_chunk(directory, {'hash': digest, 'size': len(body)}), body)

    def test_long_index_and_compaction_passes_keep_reporting(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for number in range(3):
                self.make_record(directory, number, b'record %d\n' % number)
            clock = [0]
            reports = []
            original_stat = Path.stat

            def slow_stat(path, *args, **kwargs):
                clock[0] += 10  # Each filesystem touch is slow under I/O pressure.
                return original_stat(path, *args, **kwargs)

            with closing(archive.connect(directory)) as db:
                with patch.object(archive.time, 'monotonic', lambda: clock[0]), \
                     patch.object(Path, 'stat', slow_stat):
                    archive.index(db, directory, progress=reports.append)
                    self.assertTrue(reports)
                    calls = []
                    with patch.object(dedup, 'pack_payloads',
                                      lambda *a, **k: calls.append(k.get('progress')) or 0):
                        archive.optimize(db, directory, progress=reports.append)
                    self.assertTrue(calls and all(callable(progress) for progress in calls))
                # Every record is now a manifest; a cold listing visits each one
                # before index can report, so the listing itself must heartbeat.
                beats = []
                with archive.archive_sources(directory, progress=lambda: beats.append(1)) as sources:
                    self.assertEqual(len(sources), 3)
                self.assertEqual(len(beats), 3)
                published = archive.publish_stats(
                    db, directory, now=archive.datetime(2026, 1, 1, tzinfo=archive.timezone.utc))
                touched = archive.touch_stats(directory)
                self.assertNotEqual(touched['updated_at'], published['updated_at'])
                stored = json.loads((directory / 'request-archive-stats.json').read_text())
                self.assertEqual(stored, touched)
                self.assertEqual({k: v for k, v in touched.items() if k != 'updated_at'},
                                 {k: v for k, v in published.items() if k != 'updated_at'})

    def test_deduplicated_decoder_rejects_excessive_window(self):
        import io
        compressor = zstd.ZstdCompressor(options={zstd.CompressionParameter.compression_level: 1,
                                                  zstd.CompressionParameter.window_log: 27})
        frame = compressor.compress(b'small payload with a 128 MiB declared window') + compressor.flush()
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(zstd.ZstdError):
                with dedup.decoded_payload(Path(tmp), io.BytesIO(frame)) as source:
                    source.read()


if __name__ == '__main__':
    unittest.main()
