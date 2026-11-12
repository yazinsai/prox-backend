package management

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func deduplicatedManagementFixture(t *testing.T, dir, name string, body []byte) {
	t.Helper()
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = encoder.Close() }()
	var chunks []interface{}
	// Split inside headers and body lines to exercise stream reconstruction.
	for offset := 0; offset < len(body); offset += 73 {
		end := offset + 73
		if end > len(body) {
			end = len(body)
		}
		part := body[offset:end]
		sum := sha256.Sum256(part)
		value := hex.EncodeToString(sum[:])
		path := filepath.Join(dir, "request-chunks", value[:2], value+".zst")
		if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(path, encoder.EncodeAll(part, nil), 0600); err != nil {
			t.Fatal(err)
		}
		chunks = append(chunks, map[string]interface{}{"hash": value, "size": len(part)})
	}
	sum := sha256.Sum256(body)
	doc := ojson.NewDocString(`{}`)
	_ = doc.Set("schema_version", 1)
	_ = doc.Set("name", name)
	_ = doc.Set("modified_ns", time.Now().UnixNano())
	_ = doc.Set("original_bytes", len(body))
	_ = doc.Set("legacy_compressed_bytes", len(body)/2)
	_ = doc.Set("sha256", hex.EncodeToString(sum[:]))
	_ = doc.Set("chunks", chunks)
	path := filepath.Join(dir, "request-records", name+".json.zst")
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, encoder.EncodeAll(doc.Bytes(), nil), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestDeduplicatedArchiveDownloadsAndErrorListing(t *testing.T) {
	dir := t.TempDir()
	name := "error-v1-responses-2026-11-05T150000-12345678.log.zst"
	payload := append([]byte("complete archived exchange\n"), bytes.Repeat([]byte{0, 255, 128, 10}, 10000)...)
	deduplicatedManagementFixture(t, dir, name, payload)
	h := &Handler{cfg: &config.Config{}, logDir: dir}
	for _, byID := range []bool{true, false} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/v8/management/observability/logs/requests/12345678", nil)
		if byID {
			c.Params = gin.Params{{Key: "id", Value: "12345678"}}
			h.GetRequestLogByID(c)
		} else {
			c.Params = gin.Params{{Key: "name", Value: strings.TrimSuffix(name, ".zst")}}
			h.DownloadRequestErrorLog(c)
		}
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), payload) {
			t.Fatalf("dedup download by ID=%v: status %d", byID, rec.Code)
		}
		if rec.Header().Get("Content-Disposition") != "attachment; filename=\""+strings.TrimSuffix(name, ".zst")+"\"" {
			t.Fatal("dedup changed attachment filename")
		}
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v8/management/observability/logs/errors", nil)
	h.GetRequestErrorLogs(c)
	if rec.Code != http.StatusOK || !bytes.Contains(rec.Body.Bytes(), []byte(strings.TrimSuffix(name, ".zst"))) {
		t.Fatalf("dedup error missing: %s", rec.Body.String())
	}
}

func TestDeduplicatedArchiveQuotaHistoryMatchesOriginalBytes(t *testing.T) {
	dir := t.TempDir()
	name := "v1-responses-2026-11-05T161716-a.log.zst"
	deduplicatedManagementFixture(t, dir, name, []byte(quotaHistoryRecordFixture))
	assertArchiveBackfill(t, dir)
}

func packManagementDedupFixture(t *testing.T, dir, prefix string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.zst"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no dedup objects to pack: %v", err)
	}
	output, err := os.Create(filepath.Join(dir, prefix+"test.zip"))
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	for _, path := range paths {
		data, errRead := os.ReadFile(path)
		if errRead != nil {
			t.Fatal(errRead)
		}
		member, errMember := writer.CreateHeader(&zip.FileHeader{Name: filepath.Base(path), Method: zip.Store})
		if errMember != nil {
			t.Fatal(errMember)
		}
		if _, err = member.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err = output.Close(); err != nil {
		t.Fatal(err)
	}
	for _, path := range paths {
		if err = os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDeduplicatedArchivePackedDownloadAndQuota(t *testing.T) {
	dir := t.TempDir()
	name := "error-v1-responses-2026-11-05T150000-12345678.log.zst"
	body := []byte(quotaHistoryRecordFixture)
	deduplicatedManagementFixture(t, dir, name, body)
	packManagementDedupFixture(t, filepath.Join(dir, "request-records"), "records-")
	shards, err := os.ReadDir(filepath.Join(dir, "request-chunks"))
	if err != nil {
		t.Fatal(err)
	}
	for _, shard := range shards {
		packManagementDedupFixture(t, filepath.Join(dir, "request-chunks", shard.Name()), "chunks-")
	}
	h := &Handler{cfg: &config.Config{}, logDir: dir}
	for _, byID := range []bool{true, false} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/v8/management/observability/logs/requests/12345678", nil)
		if byID {
			c.Params = gin.Params{{Key: "id", Value: "12345678"}}
			h.GetRequestLogByID(c)
		} else {
			c.Params = gin.Params{{Key: "name", Value: strings.TrimSuffix(name, ".zst")}}
			h.DownloadRequestErrorLog(c)
		}
		if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), body) {
			t.Fatalf("packed dedup download byID=%v: status %d", byID, rec.Code)
		}
	}
	assertArchiveBackfill(t, dir)
}
