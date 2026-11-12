// Package requestfacts records sanitized metadata for every upstream attempt
// as it happens, in append-only daily JSON Lines files under
// <log-dir>/request-facts. It also keeps the quota readings carried by those
// attempts in memory so quota history never has to be reconstructed from the
// compressed request archive.
package requestfacts

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	// DirectoryName is the subdirectory of the log directory holding facts.
	DirectoryName = "request-facts"
	// Retention bounds how long daily fact files are kept.
	Retention = 90 * 24 * time.Hour
	// QuotaSpan bounds the quota readings kept in memory.
	QuotaSpan = 62 * 24 * time.Hour

	factFilePrefix   = "facts-"
	factFileSuffix   = ".jsonl"
	backfillFileName = "quota-backfill.jsonl"
	dayLayout        = "2006-01-02"
	maxLineBytes     = 1 << 20
	// Codex reports reset instants with one-second jitter between responses.
	resetTolerance = 2 * time.Minute
)

// QuotaSample is one quota observation for a credential.
type QuotaSample struct {
	AuthID     string
	Provider   string
	ObservedMs int64
	Windows    []Window
}

type seriesKey struct{ authID, provider, window string }

type quotaPoint struct {
	ms      int64
	used    float64
	resetMs int64
}

type quotaSeries struct {
	minutes int
	points  []quotaPoint
}

// Store appends fact rows and serves quota readings. It is safe for
// concurrent use.
type Store struct {
	dir       string
	now       func() time.Time
	startedAt time.Time

	writeMu   sync.Mutex
	file      *os.File
	day       string
	writeWarn bool

	loadOnce sync.Once
	quotaMu  sync.Mutex
	series   map[seriesKey]*quotaSeries
}

// Open returns a store writing under logDir/request-facts. An empty logDir
// keeps quota readings in memory only. Nothing is created until the first row
// is written.
func Open(logDir string) *Store {
	s := &Store{now: time.Now, series: make(map[seriesKey]*quotaSeries)}
	if strings.TrimSpace(logDir) != "" {
		s.dir = filepath.Join(logDir, DirectoryName)
	}
	s.startedAt = s.now()
	return s
}

// HandleUsage implements usage.Plugin.
func (s *Store) HandleUsage(_ context.Context, record usage.Record) {
	s.Append(FromRecord(record))
}

// Append persists row and indexes its quota readings.
func (s *Store) Append(row Row) {
	if s == nil {
		return
	}
	if line, err := row.encode(); err == nil {
		s.write(line)
	}
	if len(row.Quota) > 0 && row.AuthID != "" && row.Provider != "" {
		s.quotaMu.Lock()
		s.addQuota(row.AuthID, row.Provider, row.At.UnixMilli(), row.Quota)
		s.quotaMu.Unlock()
	}
}

func (s *Store) write(line []byte) {
	if s.dir == "" {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	day := s.now().UTC().Format(dayLayout)
	if s.file == nil || s.day != day {
		s.rotate(day)
	}
	if s.file == nil {
		return
	}
	if _, err := s.file.Write(line); err != nil {
		s.warn("write", err)
		_ = s.file.Close()
		s.file = nil
	}
}

// rotate opens the file for day and prunes files older than Retention.
// Callers hold writeMu.
func (s *Store) rotate(day string) {
	if s.file != nil {
		_ = s.file.Close()
		s.file = nil
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		s.warn("create directory", err)
		return
	}
	file, err := os.OpenFile(filepath.Join(s.dir, factFilePrefix+day+factFileSuffix), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		s.warn("open", err)
		return
	}
	// A crash can leave a partial last line; start the next row on its own line.
	if info, errStat := file.Stat(); errStat == nil && info.Size() > 0 {
		last := make([]byte, 1)
		if _, errRead := file.ReadAt(last, info.Size()-1); errRead == nil && last[0] != '\n' {
			_, _ = file.Write([]byte{'\n'})
		}
	}
	s.file, s.day, s.writeWarn = file, day, false
	cutoff := s.now().Add(-Retention).UTC().Format(dayLayout)
	for _, name := range s.factFiles() {
		if factFileDay(name) < cutoff {
			if errRemove := os.Remove(filepath.Join(s.dir, name)); errRemove != nil && !os.IsNotExist(errRemove) {
				log.WithError(errRemove).Debug("request facts: prune expired file")
			}
		}
	}
}

func (s *Store) warn(action string, err error) {
	if !s.writeWarn {
		log.WithError(err).Warnf("request facts: %s failed", action)
		s.writeWarn = true
	}
}

// factFiles lists daily fact files in ascending day order.
func (s *Store) factFiles() []string {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() && factFileDay(name) != "" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func factFileDay(name string) string {
	if !strings.HasPrefix(name, factFilePrefix) || !strings.HasSuffix(name, factFileSuffix) {
		return ""
	}
	day := strings.TrimSuffix(strings.TrimPrefix(name, factFilePrefix), factFileSuffix)
	if _, err := time.Parse(dayLayout, day); err != nil {
		return ""
	}
	return day
}

// Recording reports whether quota readings are being collected.
func (s *Store) Recording() bool { return s != nil }

// QuotaSamples returns quota readings observed at or after since, one sample
// per stored change point, and the oldest observation time.
func (s *Store) QuotaSamples(since time.Time) ([]QuotaSample, int64) {
	if s == nil {
		return nil, 0
	}
	s.loadOnce.Do(s.load)
	cutoff := since.UnixMilli()
	expired := s.now().Add(-QuotaSpan).UnixMilli()
	s.quotaMu.Lock()
	defer s.quotaMu.Unlock()
	var samples []QuotaSample
	var oldest int64
	for key, series := range s.series {
		keep := sort.Search(len(series.points), func(i int) bool { return series.points[i].ms >= expired })
		series.points = series.points[keep:]
		if len(series.points) == 0 {
			delete(s.series, key)
			continue
		}
		for _, point := range series.points {
			if point.ms < cutoff {
				continue
			}
			if oldest == 0 || point.ms < oldest {
				oldest = point.ms
			}
			samples = append(samples, QuotaSample{
				AuthID:     key.authID,
				Provider:   key.provider,
				ObservedMs: point.ms,
				Windows:    []Window{{ID: key.window, WindowMinutes: series.minutes, UsedPercent: point.used, ResetAtMs: point.resetMs}},
			})
		}
	}
	return samples, oldest
}

// load reads persisted quota readings once. It builds the index without
// holding quotaMu, then merges readings appended meanwhile, so live traffic
// is never blocked by the initial scan.
func (s *Store) load() {
	if s.dir == "" {
		return
	}
	started := time.Now()
	cutoffMs := s.now().Add(-QuotaSpan).UnixMilli()
	cutoffDay := s.now().Add(-QuotaSpan - 24*time.Hour).UTC().Format(dayLayout)
	built := make(map[seriesKey]*quotaSeries)
	files := []string{backfillFileName}
	for _, name := range s.factFiles() {
		if factFileDay(name) >= cutoffDay {
			files = append(files, name)
		}
	}
	lines := 0
	for _, name := range files {
		lines += readQuotaFile(filepath.Join(s.dir, name), cutoffMs, built)
	}
	s.quotaMu.Lock()
	mergeSeries(built, s.series)
	s.series = built
	s.quotaMu.Unlock()
	log.Debugf("request facts: loaded %d quota readings from %d files in %s", lines, len(files), time.Since(started))
}

func readQuotaFile(path string, cutoffMs int64, into map[seriesKey]*quotaSeries) int {
	file, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.WithError(err).Warn("request facts: open file")
		}
		return 0
	}
	defer func() { _ = file.Close() }()
	reader := bufio.NewReaderSize(file, 64<<10)
	marker := []byte(`"quota"`)
	count := 0
	for {
		line, errRead := reader.ReadSlice('\n')
		if errRead == bufio.ErrBufferFull {
			// Rows are far smaller than the buffer; skip the remainder of a damaged line.
			for errRead == bufio.ErrBufferFull {
				_, errRead = reader.ReadSlice('\n')
			}
			continue
		}
		if len(line) > 0 && bytes.Contains(line, marker) {
			if authID, provider, atMs, windows, ok := decodeQuota(line); ok && atMs >= cutoffMs {
				addQuotaTo(into, authID, provider, atMs, windows)
				count++
			}
		}
		if errRead != nil {
			if errRead != io.EOF {
				log.WithError(errRead).Warn("request facts: read file")
			}
			return count
		}
	}
}

// addQuota indexes readings. Callers hold quotaMu.
func (s *Store) addQuota(authID, provider string, atMs int64, windows []Window) {
	addQuotaTo(s.series, authID, provider, atMs, windows)
}

func addQuotaTo(into map[seriesKey]*quotaSeries, authID, provider string, atMs int64, windows []Window) {
	for _, window := range windows {
		key := seriesKey{authID, provider, window.ID}
		series := into[key]
		if series == nil {
			series = &quotaSeries{}
			into[key] = series
		}
		series.minutes = window.WindowMinutes
		series.points = appendPoint(series.points, quotaPoint{atMs, window.UsedPercent, window.ResetAtMs})
	}
}

func samePoint(a, b quotaPoint) bool {
	reset := a.resetMs - b.resetMs
	if reset < 0 {
		reset = -reset
	}
	return math.Abs(a.used-b.used) < 0.05 && time.Duration(reset)*time.Millisecond <= resetTolerance
}

// appendPoint keeps only the endpoints of runs with an unchanged reading: a
// busy credential reports on every request, and repeated readings add memory
// without changing the drawn line. A reading already present (the initial
// load also sees rows indexed live) is dropped.
func appendPoint(points []quotaPoint, point quotaPoint) []quotaPoint {
	n := len(points)
	if n > 0 && point.ms < points[n-1].ms {
		index := sort.Search(n, func(i int) bool { return points[i].ms > point.ms })
		if index > 0 && points[index-1].ms == point.ms && samePoint(points[index-1], point) {
			return points
		}
		return slices.Insert(points, index, point)
	}
	if n > 0 && points[n-1].ms == point.ms && samePoint(points[n-1], point) {
		return points
	}
	if n >= 2 && samePoint(points[n-2], points[n-1]) && samePoint(points[n-1], point) {
		points[n-1] = point
		return points
	}
	return append(points, point)
}

// mergeSeries folds src into dst, keeping each series sorted and compacted.
func mergeSeries(dst, src map[seriesKey]*quotaSeries) {
	for key, incoming := range src {
		existing := dst[key]
		if existing == nil {
			dst[key] = &quotaSeries{minutes: incoming.minutes, points: append([]quotaPoint(nil), incoming.points...)}
			continue
		}
		merged := make([]quotaPoint, 0, len(existing.points)+len(incoming.points))
		merged = append(merged, existing.points...)
		merged = append(merged, incoming.points...)
		sort.SliceStable(merged, func(i, j int) bool { return merged[i].ms < merged[j].ms })
		compacted := make([]quotaPoint, 0, len(merged))
		for _, point := range merged {
			compacted = appendPoint(compacted, point)
		}
		existing.points = compacted
		if incoming.minutes > 0 {
			existing.minutes = incoming.minutes
		}
	}
}
