package gateway

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/filememo"
	"github.com/yetone/magpie/internal/provider"
)

// The client keeps the completed search items (sometimes without sources),
// even for a parallel batch. It returns its own tool result on a later request.
func TestAgySearchHistoryRoundTrip(t *testing.T) {
	h := &searchHistory{t.TempDir(), "caller A"}
	calls := []Part{{Kind: ToolCall, ID: "toolu_a", Name: "web_search", Args: json.RawMessage(`{"query":"first"}`)}, {Kind: ToolCall, ID: "toolu_b", Name: "web_search", Args: json.RawMessage(`{"query":"second"}`)}}
	results := []Part{{Kind: ToolResult, CallID: "toolu_a", Text: "full result A"}, {Kind: ToolResult, CallID: "toolu_b", Text: "full result B"}}
	events := []Event{{Kind: KSearch, Text: "first", Hits: []Hit{{URL: "https://example.org/"}}}, {Kind: KSearch, Text: "second"}}
	h.save(calls, results, events)
	files, err := os.ReadDir(h.dir)
	if err != nil || len(files) != 1 {
		t.Fatalf("cache files %d, %v", len(files), err)
	}
	path := filepath.Join(h.dir, files[0].Name())
	ciphertext, err := os.ReadFile(path)
	if err != nil || bytes.Contains(ciphertext, []byte(results[0].Text)) || bytes.Contains(ciphertext, []byte(events[0].ID)) {
		t.Fatal("search cache contains cleartext or is unreadable")
	}
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatal("empty search cache")
	}

	itemA := responsesSearchItem(events[0].ID, events[0].Text, events[0].Hits)
	itemB := responsesSearchItem(events[1].ID, events[1].Text, nil)
	encode := func(items ...any) []rItem {
		t.Helper()
		b, e := json.Marshal(items)
		if e != nil {
			t.Fatal(e)
		}
		var out []rItem
		if e = json.Unmarshal(b, &out); e != nil {
			t.Fatal(e)
		}
		return out
	}
	items := encode(itemA, itemB)
	filememo.DropKind("agy-search-round")
	cold := &searchHistory{h.dir, h.owner}
	for pass := 0; pass < 2; pass++ {
		if pass == 1 {
			items[0].Action.Sources = nil
		}
		got, n := cold.restore(items)
		if n != 2 || len(got) != 2 || !reflect.DeepEqual(got[0].Parts, calls) || !reflect.DeepEqual(got[1].Parts, results) {
			t.Fatalf("lost original search round, consumed %d", n)
		}
	}
	for _, tc := range []struct {
		name   string
		modify func([]rItem) []rItem
	}{
		{"partial", func(x []rItem) []rItem { return x[:1] }},
		{"reordered", func(x []rItem) []rItem { return []rItem{x[1], x[0]} }},
		{"edited query", func(x []rItem) []rItem { x[1].Action.Query = "edited"; return x }},
		{"edited source", func(x []rItem) []rItem { x[0].Action.Sources[0].URL = "https://different.example/"; return x }},
		{"another ID", func(x []rItem) []rItem { x[1].ID = "ws_other"; return x }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, n := h.restore(tc.modify(encode(itemA, itemB))); n != 0 {
				t.Fatal("restored a changed round")
			}
		})
	}
	other := &searchHistory{h.dir, "caller B"}
	if _, n := other.restore(items); n != 0 {
		t.Fatal("restored across owners")
	}
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, n := h.restore(items); n != 0 {
		t.Fatal("restored a damaged round")
	}
	expired := time.Now().Add(-searchIdle - time.Minute)
	if err := os.Chtimes(path, expired, expired); err != nil {
		t.Fatal(err)
	}
	pruneSearchRounds(h.dir)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("expired record retained: %v", err)
	}
}

func TestAgySearchHistoryRebuildsUnsignedResponsesInput(t *testing.T) {
	r := httptest.NewRequest("POST", "/v1/responses", nil)
	r.Header.Set("session_id", "session-a")
	h := agySearchHistory(r, provider.PluginForTest("p", "agy-cli"), "model")
	if h == nil {
		t.Fatal("missing session scope")
	}
	h.dir = t.TempDir()
	events := []Event{{Kind: KSearch, Text: "query"}}
	h.save([]Part{{Kind: ToolCall, Name: "web_search", ID: "toolu_search", Args: json.RawMessage(`{"query":"query"}`)}}, []Part{{Kind: ToolResult, CallID: "toolu_search", Text: "actual search result"}}, events)
	item := responsesSearchItem(events[0].ID, events[0].Text, nil)
	raw, _ := json.Marshal(item)
	input := `[{"role":"user","content":"ask"},` + string(raw) + `,{"type":"function_call","name":"echo","call_id":"toolu_pending","arguments":"{}"},{"type":"function_call_output","call_id":"toolu_pending","output":"client result"}]`
	q, err := readResponsesWithSearch([]byte(codexSearchAsk(true, input)), false, false, h)
	if err != nil || len(q.Messages) != 5 {
		t.Fatalf("expected five messages, got %v, %v", q, err)
	}
	if q.Messages[1].Parts[0].ID != "toolu_search" || q.Messages[2].Parts[0].Text != "actual search result" || q.Messages[4].Parts[0].CallID != "toolu_pending" {
		t.Fatal("search or pending tool history changed")
	}
	if other := agySearchHistory(r, provider.PluginForTest("p", "other"), "model"); other != nil {
		t.Fatal("enabled outside agy")
	}
	if strings.Contains(input, "actual search result") {
		t.Fatal("client needed the full search result")
	}
}
