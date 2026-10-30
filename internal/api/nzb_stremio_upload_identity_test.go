package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/kipsilabs/altmount/internal/auth"
	"github.com/kipsilabs/altmount/internal/config"
	"github.com/kipsilabs/altmount/internal/database"
	"github.com/kipsilabs/altmount/internal/importer"
	"github.com/kipsilabs/altmount/internal/metadata"
	"github.com/kipsilabs/altmount/internal/progress"
	"github.com/stretchr/testify/require"
)

const uploadIdentityTitle = "Same.Title.2024.1080p.WEB-DL"

// uploadIdentityFixture wires the upload endpoint to a real importer service
// over a temporary database, plus an indexer that serves one NZB per release
// URL. Both URLs end in the same transport filename, which is what makes the
// two releases indistinguishable by title alone.
type uploadIdentityFixture struct {
	app      *fiber.App
	key      string
	urlA     string
	urlB     string
	releaseA string
	releaseB string
	db       *database.DB
	queue    *database.Repository
}

func newUploadIdentityFixture(t *testing.T) *uploadIdentityFixture {
	t.Helper()

	indexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-nzb")
		_, _ = w.Write([]byte("<nzb></nzb>"))
	}))
	t.Cleanup(indexer.Close)

	db, err := database.NewDB(database.Config{DatabasePath: filepath.Join(t.TempDir(), "test.db")})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	userRepo := database.NewUserRepository(db.Connection(), database.DialectSQLite)
	apiKey := "test-api-key"
	require.NoError(t, userRepo.CreateUser(ctx, &database.User{UserID: "admin", Provider: "local", APIKey: &apiKey}))

	enabled := true
	cfg := &config.Config{}
	cfg.Stremio.Enabled = &enabled
	cfg.Stremio.NzbTTLHours = 24

	importerService, err := importer.NewService(
		importer.ServiceConfig{Workers: 1},
		metadata.NewMetadataService(t.TempDir()),
		db,
		nil,
		nil,
		func() *config.Config { return cfg },
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	server := &Server{
		configManager:       &mockConfigManager{cfg: cfg},
		userRepo:            userRepo,
		queueRepo:           database.NewRepository(db.Connection(), database.DialectSQLite),
		importerService:     importerService,
		progressBroadcaster: progress.NewProgressBroadcaster(),
	}
	app := fiber.New()
	app.Post("/api/nzb/streams", server.handleNzbStreams)

	return &uploadIdentityFixture{
		app:      app,
		key:      auth.HashAPIKey(apiKey),
		urlA:     indexer.URL + "/Same.Title.2024.1080p.WEB-DL.nzb?release=a",
		urlB:     indexer.URL + "/Same.Title.2024.1080p.WEB-DL.nzb?release=b",
		releaseA: stremioReleaseKey(stremioPlayCandidate{DownloadURL: indexer.URL + "/Same.Title.2024.1080p.WEB-DL.nzb?release=a"}),
		releaseB: stremioReleaseKey(stremioPlayCandidate{DownloadURL: indexer.URL + "/Same.Title.2024.1080p.WEB-DL.nzb?release=b"}),
		db:       db,
		queue:    database.NewRepository(db.Connection(), database.DialectSQLite),
	}
}

// upload posts one NZB URL to the upload endpoint. Every call uses a one second
// timeout: a queue hit waits for that timeout (no workers run), while a cached
// completed item answers immediately.
func (f *uploadIdentityFixture) upload(t *testing.T, nzbURL string) *http.Response {
	t.Helper()
	form := url.Values{
		"nzb_url":      {nzbURL},
		"download_key": {f.key},
		"timeout":      {"1"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/nzb/streams", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := f.app.Test(req, -1)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func (f *uploadIdentityFixture) rows(t *testing.T) []*database.ImportQueueItem {
	t.Helper()
	items, err := f.queue.ListQueueItems(context.Background(), nil, "", "", 50, 0, "updated_at", "desc")
	require.NoError(t, err)
	return items
}

func TestStremioUploadSameTitleDifferentURLsNeverShareQueueEntries(t *testing.T) {
	require.NotEqual(t, stremioUploadNzbFilename("Same.Title.2024.nzb",
		stremioPlayCandidate{SafeTitle: "Same.Title.2024", DownloadURL: "https://indexer/a"}),
		stremioUploadNzbFilename("Same.Title.2024.nzb",
			stremioPlayCandidate{SafeTitle: "Same.Title.2024", DownloadURL: "https://indexer/b"}),
		"different NZB URLs must not share a queue filename")
	gzName := stremioUploadNzbFilename("Same.Title.2024.nzb.gz",
		stremioPlayCandidate{SafeTitle: "Same.Title.2024", DownloadURL: "https://indexer/a"})
	require.Contains(t, gzName, "-", "gz uploads still carry the URL hash")
	require.True(t, strings.HasSuffix(gzName, ".nzb.gz"), "gz uploads keep their transport extension")

	f := newUploadIdentityFixture(t)

	// Release A is queued once; an identical re-upload joins the same entry.
	require.Equal(t, http.StatusRequestTimeout, f.upload(t, f.urlA).StatusCode)
	require.Equal(t, http.StatusRequestTimeout, f.upload(t, f.urlA).StatusCode)
	rows := f.rows(t)
	require.Len(t, rows, 1, "an identical URL re-upload must coalesce into one queue entry")
	rowA := rows[0]
	require.Equal(t, f.releaseA, mustReleaseKey(t, rowA))
	require.Contains(t, filepath.Base(rowA.NzbPath), "-", "release URL hash belongs in the queued filename")

	// Release A finishes importing.
	pathA := "/storage/release-a/" + uploadIdentityTitle + ".mkv"
	_, err := f.db.Connection().ExecContext(context.Background(),
		`UPDATE import_queue SET status = 'completed', storage_path = ?, completed_at = ? WHERE id = ?`,
		pathA, time.Now(), rowA.ID)
	require.NoError(t, err)

	// The same release still short-circuits to its own file.
	cached := f.upload(t, f.urlA)
	require.Equal(t, http.StatusOK, cached.StatusCode)
	var body StremioStreamsResponse
	require.NoError(t, json.NewDecoder(cached.Body).Decode(&body))
	require.Len(t, body.Streams, 1)
	require.Contains(t, body.Streams[0].URL, url.QueryEscape(pathA))

	// A same-titled release behind a different URL must be queued separately:
	// never served release A's file, and never attached to release A's row.
	other := f.upload(t, f.urlB)
	require.NotEqual(t, http.StatusOK, other.StatusCode,
		"a different NZB URL must not return the cached release's streams")

	rows = f.rows(t)
	require.Len(t, rows, 2, "a same-titled release behind a different URL needs its own queue entry")
	var rowB *database.ImportQueueItem
	for _, row := range rows {
		if row.ID != rowA.ID {
			rowB = row
		}
	}
	require.NotNil(t, rowB)
	require.NotEqual(t, rowA.NzbPath, rowB.NzbPath, "releases must not share a queue path")
	require.Equal(t, f.releaseB, mustReleaseKey(t, rowB))
	require.Equal(t, f.releaseA, mustReleaseKey(t, rowA))

	// A second request for release B coalesces with its own pending entry only.
	require.Equal(t, http.StatusRequestTimeout, f.upload(t, f.urlB).StatusCode)
	require.Len(t, f.rows(t), 2, "release B must not enqueue a duplicate")
}

// mustReleaseKey reads the stored release fingerprint of a queue row.
func mustReleaseKey(t *testing.T, item *database.ImportQueueItem) string {
	t.Helper()
	key, keyed := stremioQueueItemReleaseKey(item)
	require.True(t, keyed, "queue row %d carries no release key", item.ID)
	return key
}

// TestStremioUploadLegacyTitleRowDoesNotServeExplicitURL keeps the keyless-row
// rule: an explicit release request may not reuse a legacy title-keyed row that
// cannot prove it holds the requested bytes.
func TestStremioUploadLegacyTitleRowDoesNotServeExplicitURL(t *testing.T) {
	f := newUploadIdentityFixture(t)

	legacyPath := "/storage/legacy/" + uploadIdentityTitle + ".mkv"
	legacyFilename := stremioUploadNzbFilename(uploadIdentityTitle+".nzb",
		stremioPlayCandidate{SafeTitle: uploadIdentityTitle, DownloadURL: f.urlA})
	_, err := f.db.Connection().ExecContext(context.Background(),
		`INSERT INTO import_queue (nzb_path, category, priority, status, retry_count, max_retries, storage_path, completed_at)
		 VALUES (?, 'stremio', 1, 'completed', 0, 3, ?, ?)`,
		"/config/.nzbs/stremio/"+legacyFilename, legacyPath, time.Now())
	require.NoError(t, err)

	resp := f.upload(t, f.urlA)
	require.NotEqual(t, http.StatusOK, resp.StatusCode,
		"a keyless title row must not answer an explicit release request")
	require.Len(t, f.rows(t), 2, "the explicit release must get its own queue entry")
}
