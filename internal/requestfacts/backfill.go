package requestfacts

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	log "github.com/sirupsen/logrus"
)

// Archived lines longer than this are request/response bodies; metadata and
// header lines are always far shorter, so long lines are skipped.
const archiveMaxLine = 64 * 1024

// Backfill imports quota readings from the compressed request archive for the
// span before this store started recording, once. Completion is marked by
// writing quota-backfill.jsonl; an interrupted run starts over on the next
// start. It is a no-op when the archive holds no records.
func (s *Store) Backfill(ctx context.Context, archiveDir string) {
	if s == nil || s.dir == "" || strings.TrimSpace(archiveDir) == "" {
		return
	}
	target := filepath.Join(s.dir, backfillFileName)
	if _, err := os.Stat(target); err == nil || !os.IsNotExist(err) {
		return
	}
	until := s.startedAt
	since := until.Add(-QuotaSpan)
	entries, err := logging.ListRequestLogFiles(archiveDir)
	if err != nil {
		if !os.IsNotExist(err) {
			log.WithError(err).Warn("request facts: list request archive for backfill")
		}
		return
	}
	var names []string
	for _, entry := range entries {
		// A record's mtime is when the request finished. Later records were
		// recorded live by this store.
		if strings.HasSuffix(entry.Name, ".log.zst") && !entry.Modified.Before(since) && entry.Modified.Before(until) {
			names = append(names, entry.Name)
		}
	}
	if len(names) == 0 {
		return
	}
	started := time.Now()
	log.Infof("request facts: backfilling quota history from %d archived requests", len(names))

	// Leave most cores to live traffic; this runs once in the background.
	workers := runtime.NumCPU() / 2
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	jobs := make(chan string)
	var mu sync.Mutex
	var samples []QuotaSample
	var skipped atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for name := range jobs {
				parsed, errParse := parseArchiveFile(filepath.Join(archiveDir, name))
				if errParse != nil {
					skipped.Add(1)
					continue
				}
				mu.Lock()
				samples = append(samples, parsed...)
				mu.Unlock()
			}
		}()
	}
feed:
	for _, name := range names {
		select {
		case <-ctx.Done():
			break feed
		case jobs <- name:
		}
	}
	close(jobs)
	wg.Wait()
	if ctx.Err() != nil {
		return
	}

	if errWrite := s.writeBackfill(target, samples); errWrite != nil {
		log.WithError(errWrite).Warn("request facts: write quota backfill")
		return
	}
	imported := make(map[seriesKey]*quotaSeries)
	for _, sample := range samples {
		addQuotaTo(imported, sample.AuthID, sample.Provider, sample.ObservedMs, sample.Windows)
	}
	// load may run concurrently; both orders converge because merging is idempotent.
	s.quotaMu.Lock()
	mergeSeries(s.series, imported)
	s.quotaMu.Unlock()
	log.Infof("request facts: backfilled %d quota readings from %d archived requests in %s (%d unreadable)",
		len(samples), len(names), time.Since(started).Round(time.Second), skipped.Load())
}

// writeBackfill writes samples atomically, so the marker file only exists
// once the backfill is complete.
func (s *Store) writeBackfill(target string, samples []QuotaSample) error {
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(s.dir, backfillFileName+".*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(temp.Name()) }()
	writer := bufio.NewWriter(temp)
	for _, sample := range samples {
		line, errEncode := Row{At: time.UnixMilli(sample.ObservedMs), RequestedAt: time.UnixMilli(sample.ObservedMs), AuthID: sample.AuthID, Provider: sample.Provider, Quota: sample.Windows}.encode()
		if errEncode != nil {
			_ = temp.Close()
			return errEncode
		}
		if _, errWrite := writer.Write(line); errWrite != nil {
			_ = temp.Close()
			return errWrite
		}
	}
	if errFlush := writer.Flush(); errFlush != nil {
		_ = temp.Close()
		return errFlush
	}
	if errSync := temp.Sync(); errSync != nil {
		_ = temp.Close()
		return errSync
	}
	if errClose := temp.Close(); errClose != nil {
		return errClose
	}
	return os.Rename(temp.Name(), target)
}

func parseArchiveFile(path string) ([]QuotaSample, error) {
	file, err := logging.OpenRequestRecord(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := file.Close(); errClose != nil {
			log.WithError(errClose).Debug("request facts: close archive")
		}
	}()
	return parseArchiveRecord(file)
}

type archiveAttempt struct {
	provider string
	authID   string
}

// parseArchiveRecord extracts quota observations from one archived request log.
//
// The log lists every upstream attempt's request ("=== API REQUEST n ===",
// carrying the Auth line) before the responses ("=== API RESPONSE n ===",
// carrying status and headers). Reading stops at the downstream
// "=== RESPONSE ===" section, so the client-facing body is never decompressed.
func parseArchiveRecord(source io.Reader) ([]QuotaSample, error) {
	reader := bufio.NewReaderSize(source, archiveMaxLine)
	attempts := make(map[int]archiveAttempt)

	const (
		sectionNone = iota
		sectionRequest
		sectionResponse
	)
	section := sectionNone
	index := 0
	requestMetaDone := false
	headerState := 0 // 0 before "Headers:", 1 reading headers, 2 done
	var responseMs int64
	var headers http.Header
	var samples []QuotaSample

	flush := func() {
		if section != sectionResponse || len(headers) == 0 {
			return
		}
		attempt, ok := attempts[index]
		if !ok || attempt.authID == "" || responseMs == 0 {
			return
		}
		windows := QuotaWindows(attempt.provider, headers)
		if len(windows) == 0 {
			return
		}
		samples = append(samples, QuotaSample{AuthID: attempt.authID, Provider: attempt.provider, ObservedMs: responseMs, Windows: windows})
	}

	skipping := false
	for {
		raw, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			skipping = true
			continue
		}
		if skipping {
			// Tail of an oversized body line.
			skipping = false
			if err != nil && !errors.Is(err, io.EOF) {
				return samples, err
			}
			if errors.Is(err, io.EOF) {
				break
			}
			continue
		}
		line := string(bytes.TrimRight(raw, "\r\n"))

		if strings.HasPrefix(line, "=== ") && strings.HasSuffix(line, " ===") {
			if line == "=== RESPONSE ===" {
				flush()
				return samples, nil
			}
			if n, ok := archiveSectionIndex(line, "=== API REQUEST "); ok {
				flush()
				section, index, requestMetaDone = sectionRequest, n, false
			} else if n, ok := archiveSectionIndex(line, "=== API RESPONSE "); ok {
				flush()
				section, index, headerState, responseMs, headers = sectionResponse, n, 0, 0, nil
			}
		} else {
			switch section {
			case sectionRequest:
				if requestMetaDone {
					break
				}
				if line == "Headers:" || line == "Body:" {
					requestMetaDone = true
				} else if rest, ok := strings.CutPrefix(line, "Auth: "); ok {
					attempts[index] = parseArchiveAuth(rest)
				}
			case sectionResponse:
				switch headerState {
				case 0:
					if rest, ok := strings.CutPrefix(line, "Timestamp: "); ok {
						if at, errTime := time.Parse(time.RFC3339Nano, strings.TrimSpace(rest)); errTime == nil {
							responseMs = at.UnixMilli()
						}
					} else if line == "Headers:" {
						headerState = 1
						headers = make(http.Header)
					}
				case 1:
					if line == "" {
						headerState = 2
					} else if key, value, ok := strings.Cut(line, ":"); ok {
						headers.Add(strings.TrimSpace(key), strings.TrimSpace(value))
					}
				}
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return samples, err
		}
	}
	flush()
	return samples, nil
}

func archiveSectionIndex(line, prefix string) (int, bool) {
	rest, ok := strings.CutPrefix(line, prefix)
	if !ok {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSuffix(rest, " ==="))
	return n, err == nil && n > 0
}

func parseArchiveAuth(line string) archiveAttempt {
	var attempt archiveAttempt
	for _, part := range strings.Split(line, ", ") {
		key, value, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		switch key {
		case "provider":
			attempt.provider = strings.ToLower(strings.TrimSpace(value))
		case "auth_id":
			attempt.authID = strings.TrimSpace(value)
		}
	}
	return attempt
}
