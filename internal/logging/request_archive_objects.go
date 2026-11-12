package logging

import (
	"archive/zip"
	"container/list"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Bounds sized to keep a multi-week archive (about 800k chunks in 8k packs)
// fully cached. Exceeding them evicts the least recently used packs only.
const (
	maxRequestPackCacheMembers = 1_000_000
	maxRequestPackCacheEntries = 16384
)

type requestPackedMember struct {
	offset int64
	size   int64
	crc    uint32
}

type requestPackMetadata struct {
	info       os.FileInfo
	modifiedNs int64
	size       int64
	members    map[string]requestPackedMember
}

// requestArchiveLRU bounds archive metadata by total members and entries and
// evicts least recently used entries. Clearing everything on overflow made an
// archive larger than the bound thrash: almost every chunk read reparsed the
// central directories of dozens of packs.
type requestArchiveLRU[V any] struct {
	sync.Mutex
	directory string
	order     *list.List
	entries   map[string]*list.Element
	members   int
	// Zero limits use the package bounds; tests set small ones.
	maxMembers, maxEntries int
}

type requestArchiveLRUItem[V any] struct {
	key     string
	value   V
	members int
}

// scope drops entries from another archive directory. Callers hold the lock.
func (cache *requestArchiveLRU[V]) scope(directory string) {
	if cache.entries == nil || cache.directory != directory {
		cache.directory = directory
		cache.order = list.New()
		cache.entries = make(map[string]*list.Element)
		cache.members = 0
	}
}

func (cache *requestArchiveLRU[V]) get(key string) (V, bool) {
	element, exists := cache.entries[key]
	if !exists {
		var zero V
		return zero, false
	}
	cache.order.MoveToFront(element)
	return element.Value.(*requestArchiveLRUItem[V]).value, true
}

func (cache *requestArchiveLRU[V]) put(key string, value V, members int) {
	maxMembers, maxEntries := cache.maxMembers, cache.maxEntries
	if maxMembers == 0 {
		maxMembers = maxRequestPackCacheMembers
	}
	if maxEntries == 0 {
		maxEntries = maxRequestPackCacheEntries
	}
	cache.remove(key)
	if members > maxMembers {
		return
	}
	cache.entries[key] = cache.order.PushFront(&requestArchiveLRUItem[V]{key: key, value: value, members: members})
	cache.members += members
	for cache.members > maxMembers || len(cache.entries) > maxEntries {
		cache.remove(cache.order.Back().Value.(*requestArchiveLRUItem[V]).key)
	}
}

func (cache *requestArchiveLRU[V]) remove(key string) {
	element, exists := cache.entries[key]
	if !exists {
		return
	}
	cache.members -= element.Value.(*requestArchiveLRUItem[V]).members
	cache.order.Remove(element)
	delete(cache.entries, key)
}

// Packs are immutable. Cache their offsets, never their open files or payloads.
var requestPackCache requestArchiveLRU[requestPackMetadata]

func openRequestPack(root *os.Root, path string) (*os.File, os.FileInfo, map[string]requestPackedMember, error) {
	file, errOpen := root.Open(path)
	if errOpen != nil {
		return nil, nil, nil, errOpen
	}
	info, errStat := file.Stat()
	if errStat != nil {
		_ = file.Close()
		return nil, nil, nil, errStat
	}
	key := filepath.Join(root.Name(), path)
	requestPackCache.Lock()
	requestPackCache.scope(root.Name())
	metadata, cached := requestPackCache.get(key)
	requestPackCache.Unlock()
	if cached && os.SameFile(metadata.info, info) && metadata.modifiedNs == info.ModTime().UnixNano() && metadata.size == info.Size() {
		return file, info, metadata.members, nil
	}
	pack, errPack := zip.NewReader(file, info.Size())
	if errPack != nil {
		_ = file.Close()
		return nil, nil, nil, errors.New("invalid request archive pack")
	}
	if len(pack.File) > maxRequestManifestParts {
		_ = file.Close()
		return nil, nil, nil, errors.New("too many request archive pack members")
	}
	members := make(map[string]requestPackedMember, len(pack.File))
	for _, member := range pack.File {
		maxObjectBytes := uint64(maxRequestChunkBytes + (1 << 20))
		validName := filepath.Base(member.Name) == member.Name && !strings.ContainsAny(member.Name, `/\\`)
		if filepath.Dir(path) == requestManifestDirectory {
			maxObjectBytes = maxRequestManifestBytes + (1 << 20)
			logicalName := strings.TrimSuffix(member.Name, requestManifestSuffix)
			validName = validName && strings.HasSuffix(member.Name, ".log.zst"+requestManifestSuffix) && !strings.Contains(logicalName, "..")
		} else if strings.HasPrefix(path, requestChunkDirectory+string(os.PathSeparator)) {
			chunkHash := strings.TrimSuffix(member.Name, ".zst")
			validName = validName && strings.HasSuffix(member.Name, ".zst") && validRequestHash(chunkHash) && strings.HasPrefix(chunkHash, filepath.Base(filepath.Dir(path)))
		}
		if !validName || member.Method != zip.Store || member.CompressedSize64 > maxObjectBytes || member.CompressedSize64 > math.MaxInt64 || member.CompressedSize64 != member.UncompressedSize64 {
			_ = file.Close()
			return nil, nil, nil, errors.New("invalid request archive pack member")
		}
		offset, errOffset := member.DataOffset()
		if errOffset != nil || offset < 0 || int64(member.CompressedSize64) > info.Size()-offset {
			_ = file.Close()
			return nil, nil, nil, errors.New("invalid request archive pack offset")
		}
		if _, exists := members[member.Name]; exists {
			_ = file.Close()
			return nil, nil, nil, errors.New("duplicate request archive pack member")
		}
		members[member.Name] = requestPackedMember{offset: offset, size: int64(member.CompressedSize64), crc: member.CRC32}
	}
	requestPackCache.Lock()
	requestPackCache.scope(root.Name())
	requestPackCache.put(key, requestPackMetadata{info: info, modifiedNs: info.ModTime().UnixNano(), size: info.Size(), members: members}, len(members))
	requestPackCache.Unlock()
	return file, info, members, nil
}

type requestPackLookupMetadata struct {
	info    os.FileInfo
	members map[string]string
}

// Directory identity and metadata detect atomic pack publication/removal. The
// index locates one containing pack per object and retains no file descriptors.
var requestPackLookupCache requestArchiveLRU[requestPackLookupMetadata]

func requestPackLookup(root *os.Root, directory, prefix string) (map[string]string, error) {
	info, errStat := root.Stat(directory)
	if errStat != nil {
		return nil, errStat
	}
	key := directory + "\x00" + prefix
	requestPackLookupCache.Lock()
	defer requestPackLookupCache.Unlock()
	requestPackLookupCache.scope(root.Name())
	old, cached := requestPackLookupCache.get(key)
	if cached && os.SameFile(old.info, info) && old.info.ModTime().Equal(info.ModTime()) && old.info.Size() == info.Size() {
		return old.members, nil
	}
	paths, errPaths := requestPackPaths(root, directory, prefix)
	if errPaths != nil {
		return nil, errPaths
	}
	lookup := make(map[string]string)
	for _, path := range paths {
		file, _, members, errPack := openRequestPack(root, path)
		if os.IsNotExist(errPack) {
			continue
		}
		if errPack != nil {
			return nil, errPack
		}
		for name := range members {
			if _, exists := lookup[name]; !exists {
				lookup[name] = path
			}
		}
		if errClose := file.Close(); errClose != nil {
			return nil, errClose
		}
	}
	requestPackLookupCache.put(key, requestPackLookupMetadata{info: info, members: lookup}, len(lookup))
	return lookup, nil
}

func invalidateRequestPackLookup(root *os.Root, directory, prefix string) {
	requestPackLookupCache.Lock()
	defer requestPackLookupCache.Unlock()
	if requestPackLookupCache.directory != root.Name() {
		return
	}
	requestPackLookupCache.remove(directory + "\x00" + prefix)
}

type storedRequestMemberReader struct {
	reader   *io.SectionReader
	file     *os.File
	checksum uint32
	expected uint32
	terminal error
}

func (reader *storedRequestMemberReader) Read(buffer []byte) (int, error) {
	if reader.terminal != nil {
		return 0, reader.terminal
	}
	n, errRead := reader.reader.Read(buffer)
	if n > 0 {
		reader.checksum = crc32.Update(reader.checksum, crc32.IEEETable, buffer[:n])
	}
	if errRead == io.EOF {
		if reader.checksum != reader.expected {
			reader.terminal = errors.New("request archive pack checksum mismatch")
		} else {
			reader.terminal = io.EOF
		}
		return n, reader.terminal
	}
	return n, errRead
}

func (reader *storedRequestMemberReader) Close() error { return reader.file.Close() }

func requestPackPaths(root *os.Root, directory, prefix string) ([]string, error) {
	file, errOpen := root.Open(directory)
	if errOpen != nil {
		return nil, errOpen
	}
	defer func() { _ = file.Close() }()
	names, errEntries := file.Readdirnames(-1)
	if errEntries != nil {
		return nil, errEntries
	}
	var paths []string
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, ".zip") {
			continue
		}
		path := filepath.Join(directory, name)
		info, errInfo := root.Lstat(path)
		if os.IsNotExist(errInfo) {
			continue
		}
		if errInfo != nil {
			return nil, errInfo
		}
		if !info.IsDir() {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func openPackedRequestObject(root *os.Root, directory, prefix, name string) (io.ReadCloser, error) {
	for attempt := 0; attempt < 2; attempt++ {
		lookup, errLookup := requestPackLookup(root, directory, prefix)
		if errLookup != nil {
			return nil, errLookup
		}
		path, exists := lookup[name]
		if !exists {
			return nil, os.ErrNotExist
		}
		file, _, members, errPack := openRequestPack(root, path)
		if os.IsNotExist(errPack) {
			invalidateRequestPackLookup(root, directory, prefix)
			continue
		}
		if errPack != nil {
			return nil, errPack
		}
		member, exists := members[name]
		if !exists {
			_ = file.Close()
			invalidateRequestPackLookup(root, directory, prefix)
			continue
		}
		return &storedRequestMemberReader{reader: io.NewSectionReader(file, member.offset, member.size), file: file, expected: member.crc}, nil
	}
	return nil, os.ErrNotExist
}

// A published pack wins over its loose source during durable cleanup. Retrying
// after a missing loose source handles publication between the directory scan
// and opening without extracting or keeping unrelated pack files open.
func openRequestObject(root *os.Root, directory, prefix, name string) (io.ReadCloser, error) {
	packed, errPacked := openPackedRequestObject(root, directory, prefix, name)
	if errPacked == nil || !os.IsNotExist(errPacked) {
		return packed, errPacked
	}
	file, errOpen := root.Open(filepath.Join(directory, name))
	if errOpen == nil || !os.IsNotExist(errOpen) {
		return file, errOpen
	}
	invalidateRequestPackLookup(root, directory, prefix)
	return openPackedRequestObject(root, directory, prefix, name)
}
