package logging

import (
	"archive/zip"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestArchivePackPathsFiltersNamesAndDirectories(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"chunks-b.zip", "chunks-a.zip", "loose.zst", "records-a.zip", "chunks-partial.tmp"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(dir, "chunks-directory.zip"), 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	paths, err := requestPackPaths(root, ".", "chunks-")
	if err != nil || !slices.Equal(paths, []string{"chunks-a.zip", "chunks-b.zip"}) {
		t.Fatalf("pack paths: %v %v", paths, err)
	}
}

func archiveLookupPackFixture(t *testing.T, dir, packName, memberName string, body []byte) {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	file, err := os.Create(filepath.Join(dir, packName))
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	member, err := writer.CreateHeader(&zip.FileHeader{Name: memberName, Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = member.Write(body); err != nil {
		t.Fatal(err)
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = file.Close(); err != nil {
		t.Fatal(err)
	}
}

func archiveLookupRead(t *testing.T, root *os.Root, directory, name string, want []byte) {
	t.Helper()
	reader, err := openRequestObject(root, directory, "chunks-", name)
	if err != nil {
		t.Fatal(err)
	}
	got, errRead := io.ReadAll(reader)
	errClose := reader.Close()
	if errRead != nil || errClose != nil || !bytes.Equal(got, want) {
		t.Fatalf("packed lookup: read=%v close=%v", errRead, errClose)
	}
}

func TestArchivePackLookupDetectsPublicationAndRemoval(t *testing.T) {
	dir := t.TempDir()
	archiveLookupPackFixture(t, dir, "chunks-first.zip", "first.zst", []byte("first"))
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	archiveLookupRead(t, root, ".", "first.zst", []byte("first"))
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	archiveLookupPackFixture(t, dir, "chunks-second.zip", "second.zst", []byte("second"))
	changed := info.ModTime().Add(time.Second)
	if err = os.Chtimes(dir, changed, changed); err != nil {
		t.Fatal(err)
	}
	archiveLookupRead(t, root, ".", "second.zst", []byte("second"))
	if err = os.WriteFile(filepath.Join(dir, "second.zst"), []byte("loose recovery source"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(filepath.Join(dir, "chunks-second.zip")); err != nil {
		t.Fatal(err)
	}
	changed = changed.Add(time.Second)
	if err = os.Chtimes(dir, changed, changed); err != nil {
		t.Fatal(err)
	}
	archiveLookupRead(t, root, ".", "second.zst", []byte("loose recovery source"))
}

func TestArchivePackLookupOpensOnlyContainingPack(t *testing.T) {
	dir := t.TempDir()
	archiveLookupPackFixture(t, dir, "chunks-first.zip", "first.zst", []byte("first"))
	archiveLookupPackFixture(t, dir, "chunks-second.zip", "second.zst", []byte("second"))
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	archiveLookupRead(t, root, ".", "first.zst", []byte("first"))
	// In-place damage does not change directory entries. A cached lookup must
	// inspect only its containing pack, while opening damaged content still fails.
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "chunks-second.zip"), []byte("corrupt pack"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Chtimes(dir, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	archiveLookupRead(t, root, ".", "first.zst", []byte("first"))
	if reader, errOpen := openRequestObject(root, ".", "chunks-", "second.zst"); errOpen == nil {
		_ = reader.Close()
		t.Fatal("damaged containing pack opened")
	}
}

func TestArchivePackLookupClearsOnConnectionChange(t *testing.T) {
	for _, body := range []string{"first archive", "second archive"} {
		dir := t.TempDir()
		archiveLookupPackFixture(t, dir, "chunks-same.zip", "same.zst", []byte(body))
		root, err := os.OpenRoot(dir)
		if err != nil {
			t.Fatal(err)
		}
		archiveLookupRead(t, root, ".", "same.zst", []byte(body))
		requestPackLookupCache.Lock()
		matches := requestPackLookupCache.directory == root.Name() && requestPackLookupCache.members == 1
		requestPackLookupCache.Unlock()
		if !matches {
			t.Fatal("connection change retained another archive's pack lookup")
		}
		if err = root.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestArchivePackLookupRetriesMissingContainer(t *testing.T) {
	dir := t.TempDir()
	archiveLookupPackFixture(t, dir, "chunks-old.zip", "same.zst", []byte("original"))
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	archiveLookupRead(t, root, ".", "same.zst", []byte("original"))
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	archiveLookupPackFixture(t, dir, "chunks-new.zip", "same.zst", []byte("replacement"))
	if err = os.Remove(filepath.Join(dir, "chunks-old.zip")); err != nil {
		t.Fatal(err)
	}
	// Preserve the coarse directory signature to force the missing-pack retry.
	if err = os.Chtimes(dir, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	archiveLookupRead(t, root, ".", "same.zst", []byte("replacement"))
}

func TestArchiveLRUEvictsOnlyLeastRecentlyUsed(t *testing.T) {
	cache := requestArchiveLRU[int]{maxMembers: 3, maxEntries: 3}
	cache.scope("archive")
	for i, key := range []string{"a", "b", "c"} {
		cache.put(key, i, 1)
	}
	if _, ok := cache.get("a"); !ok {
		t.Fatal("a missing before eviction")
	}
	cache.put("d", 3, 1)
	if _, ok := cache.get("b"); ok {
		t.Fatal("least recently used entry survived")
	}
	for _, key := range []string{"a", "c", "d"} {
		if _, ok := cache.get(key); !ok {
			t.Fatalf("overflow evicted recently used entry %s", key)
		}
	}
	cache.put("big", 4, 2)
	if cache.members != 3 || len(cache.entries) != 2 {
		t.Fatalf("member bound: members=%d entries=%d", cache.members, len(cache.entries))
	}
	cache.put("huge", 5, 4)
	if _, ok := cache.get("huge"); ok || cache.members > 3 {
		t.Fatal("entry above the member bound was cached")
	}
	cache.scope("other")
	if len(cache.entries) != 0 || cache.members != 0 {
		t.Fatal("directory change retained entries")
	}
}
