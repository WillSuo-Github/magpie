package gateway

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/yetone/magpie/internal/appdir"
	"github.com/yetone/magpie/internal/filememo"
	"github.com/yetone/magpie/internal/provider"
	"github.com/yetone/magpie/internal/steady"
)

// Codex returns a search item's ID and query, but can omit its sources.
// agy saw the full tool result and checks it on every later tool turn.
type searchHistory struct {
	dir, owner string
}

func agySearchHistory(r *http.Request, p provider.Provider, model string) *searchHistory {
	session := sessionOf(r.Header)
	if session == "" || p.PluginProvider() != "agy-cli" {
		return nil
	}
	scope, _ := json.Marshal([]string{prevScope(r), session, subOf(r.Header), p.ID, p.PluginAccountKey(), model})
	return &searchHistory{filepath.Join(appdir.Cache(), "agy-search-rounds"), string(scope)}
}

type savedSearch struct {
	ID, Query, Name, CallID, Result string
	Args                            json.RawMessage
	URLs                            []string
}

const searchIDPrefix = "ws_mg1_"
const maxSearchRound = 1 << 20
const searchCacheSize = 16 << 20
const maxSearchRounds = 2048
const searchIdle = 6 * time.Hour // agy-cli's default parked session lifetime

// The client holds the random key in its ordinary opaque search ID. The
// cache holds only ciphertext. A different caller, account or session cannot
// name the file, and clearing the cache falls back to ordinary translation.
func (h *searchHistory) file(key []byte) (string, cipher.AEAD) {
	hash := sha256.New()
	hash.Write([]byte(h.owner))
	hash.Write(key)
	block, _ := aes.NewCipher(key) // both callers use exactly 32 bytes
	box, _ := cipher.NewGCMWithRandomNonce(block)
	return filepath.Join(h.dir, hex.EncodeToString(hash.Sum(nil))), box
}

// A batch of parallel calls is one assistant message followed by one user
// message. Only publish references when the entire batch is represented.
func (h *searchHistory) save(calls, results []Part, events []Event) {
	if h == nil || len(events) == 0 {
		return
	}
	items := make([]savedSearch, len(events))
	for i, ev := range events {
		if ev.Kind != KSearch {
			return
		}
		items[i] = savedSearch{Query: ev.Text, Name: calls[i].Name, CallID: calls[i].ID, Args: calls[i].Args, Result: results[i].Text}
		for _, hit := range ev.Hits {
			if hit.URL != "" {
				items[i].URLs = append(items[i].URLs, hit.URL)
			}
		}
	}
	var key [32]byte
	rand.Read(key[:])
	items[0].ID = searchIDPrefix + base64.RawURLEncoding.EncodeToString(key[:])
	for i := 1; i < len(items); i++ {
		items[i].ID = searchIDPrefix + rand.Text()
	}
	data, err := json.Marshal(items)
	if err != nil || len(data) > maxSearchRound {
		return
	}
	path, box := h.file(key[:])
	if os.MkdirAll(h.dir, 0700) != nil || steady.WriteFile(path, box.Seal(nil, nil, data, nil), 0600) != nil {
		return
	}
	pruneSearchRounds(h.dir)
	for i, item := range items {
		events[i].ID = item.ID
	}
}

func (h *searchHistory) restore(items []rItem) ([]Message, int) {
	if h == nil {
		return nil, 0
	}
	encoded, ok := strings.CutPrefix(items[0].ID, searchIDPrefix)
	if !ok || len(encoded) != base64.RawURLEncoding.EncodedLen(32) {
		return nil, 0
	}
	key, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return nil, 0
	}
	path, box := h.file(key)
	stat, err := os.Stat(path)
	if err != nil || stat.Size() > maxSearchRound+int64(box.Overhead()) || time.Since(stat.ModTime()) > searchIdle {
		return nil, 0
	}
	round, err := filememo.ReadNow("agy-search-round", path, func(b []byte) ([]savedSearch, error) {
		data, err := box.Open(nil, nil, b, nil)
		if err != nil {
			return nil, err
		}
		var saved []savedSearch
		err = json.Unmarshal(data, &saved)
		return saved, err
	})
	if err != nil || len(round) == 0 || len(round) > len(items) {
		return nil, 0
	}
	for i, saved := range round {
		it := items[i]
		if it.Type != "web_search_call" || it.ID != saved.ID || it.Action == nil || it.Action.Query != saved.Query {
			return nil, 0
		}
		if (it.Status != "" && it.Status != "completed") || (it.Action.Type != "" && it.Action.Type != "search") ||
			(len(it.Action.Queries) > 0 && (len(it.Action.Queries) != 1 || it.Action.Queries[0] != saved.Query)) {
			return nil, 0
		}
		if len(it.Action.Sources) > 0 {
			if len(it.Action.Sources) != len(saved.URLs) {
				return nil, 0
			}
			for j, source := range it.Action.Sources {
				if source.URL != saved.URLs[j] {
					return nil, 0
				}
			}
		}
	}
	if time.Since(stat.ModTime()) > time.Hour {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
	}
	calls, results := Message{Role: "assistant"}, Message{Role: "user"}
	for _, s := range round {
		calls.Parts = append(calls.Parts, Part{Kind: ToolCall, ID: s.CallID, Name: s.Name, Args: s.Args})
		results.Parts = append(results.Parts, Part{Kind: ToolResult, CallID: s.CallID, Text: s.Result})
	}
	return []Message{calls, results}, len(round)
}

func pruneSearchRounds(dir string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	var files []os.FileInfo
	for _, entry := range entries {
		if len(entry.Name()) != sha256.Size*2 {
			continue
		}
		if fi, err := entry.Info(); err == nil && fi.Mode().IsRegular() {
			files = append(files, fi)
		}
	}
	slices.SortFunc(files, func(a, b os.FileInfo) int { return b.ModTime().Compare(a.ModTime()) })
	var bytes int64
	for i, f := range files {
		bytes += f.Size()
		if i >= maxSearchRounds || bytes > searchCacheSize || time.Since(f.ModTime()) > searchIdle {
			_ = os.Remove(filepath.Join(dir, f.Name()))
		}
	}
}
