package plugin

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"testing"
	"time"
)

// heavyFor sets the host's memory bound for one test (heavyMB and its
// timing), checked every 20ms from the host's start.
func heavyFor(t *testing.T, mb int) {
	t.Helper()
	was, after, every := heavyMB, heavyAfter, heavyEvery
	heavyMB, heavyAfter, heavyEvery = mb, 0, 20*time.Millisecond
	t.Cleanup(func() { heavyMB, heavyAfter, heavyEvery = was, after, every })
}

func currentHost() *host {
	hostMu.Lock()
	defer hostMu.Unlock()
	return current
}

// A host holding more than heavyMB after a full collection is replaced:
// it ends, and the next call is answered by a new host. A host that grew
// with what its plugins kept was otherwise left to grow until the system
// killed it (783 MiB after 2h25m, OOM-killed on a small Linux server).
// Any host holds more than 1 MiB.
func TestHeavyHostIsReplaced(t *testing.T) {
	heavyFor(t, 1) // restored after sandbox's Settle has ended the hosts
	sandbox(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	abs, _ := filepath.Abs("testdata/fake/index.js")
	if _, err := Add(ctx, abs); err != nil {
		t.Fatal(err)
	}
	was := recycles.Load()
	if _, err := Providers(ctx); err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(30 * time.Second)
	for recycles.Load() == was {
		if time.Now().After(until) {
			t.Fatal("a host past its memory bound was not replaced 30s on")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// the one replaced ends, and a new host answers (replaced in its turn,
	// past the bound too)
	until = time.Now().Add(30 * time.Second)
	for {
		hostMu.Lock()
		n := len(retiring)
		hostMu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(until) {
			t.Fatal("the replaced host was still finishing its calls 30s on")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := Call(ctx, "providers", map[string]any{}, nil); err != nil {
		t.Fatalf("a call after the heavy host was replaced: %v", err)
	}
}

// A host under the bound is left alone, however often it checks.
func TestLightHostIsKept(t *testing.T) {
	heavyFor(t, 1<<20)
	sandbox(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	abs, _ := filepath.Abs("testdata/fake/index.js")
	if _, err := Add(ctx, abs); err != nil {
		t.Fatal(err)
	}
	was := recycles.Load()
	if _, err := Providers(ctx); err != nil {
		t.Fatal(err)
	}
	h := currentHost()
	if h == nil {
		t.Fatal("no host")
	}
	// fifty checks
	time.Sleep(time.Second)
	if recycles.Load() != was || currentHost() != h || !h.alive() {
		t.Fatal("a host under its memory bound was replaced")
	}
}

// The plugin holds its upstream call across two client HTTP requests.
// Replacing the heavy host must let a new conversation start elsewhere
// while the client can still deliver the tool result to its owner.
func TestHeavyHostToolContinuation(t *testing.T) {
	ask := toolLoopPlugin(t, nil)
	id := ask("A", "{}")
	original := currentHost()
	original.recycle()
	b := ask("B", "{}")
	if currentHost() == original {
		t.Fatal("new conversation stayed on heavy host")
	}
	result := fmt.Sprintf(`{"result":%q}`, id)
	if got := ask("B", result); got != "no live call" {
		t.Fatalf("unrelated conversation reached old call: %q", got)
	}
	if got := ask("A", result); got != "finished" {
		t.Fatalf("result missed the host holding its tool call: %q", got)
	}
	if got := ask("A", result); got != "no live call" {
		t.Fatalf("result delivered twice: %q", got)
	}
	if got := ask("B", fmt.Sprintf(`{"result":%q}`, b)); got != "finished" {
		t.Fatalf("new conversation lost its call: %q", got)
	}
}

func TestHostConversationBounds(t *testing.T) {
	var book recentConversations
	now := time.Now()
	request := FetchRequest{Provider: "p", Account: "a", Session: "fallback", Headers: map[string]string{"x-magpie-session": "explicit", "x-magpie-subagent": "child"}}
	key := request.conversationKey()
	if key != (conversationKey{"p", "a", "explicit", "child"}) {
		t.Fatalf("session key = %+v", key)
	}
	held := book.take(key, now, true)
	for i := range maxHostConversations {
		other := key
		other.session = fmt.Sprint(i)
		book.take(other, now, true).finish()
	}
	if len(book.entries) != maxHostConversations {
		t.Fatalf("kept %d conversations", len(book.entries))
	}
	late := now.Add(time.Hour)
	c := book.take(key, late, false)
	if c != held || c == nil {
		t.Fatal("evicted a conversation still answering a request")
	}
	c.finish()
	held.finish()
	if book.take(key, late, false) != nil || book.live(late, false) {
		t.Fatal("idle conversations still hold the old host")
	}
}

func TestRestartForgetsHeavyHost(t *testing.T) {
	ask := toolLoopPlugin(t, nil)
	id := ask("A", "{}")
	currentHost().recycle()
	Restart()
	if got := ask("A", fmt.Sprintf(`{"result":%q}`, id)); got != "no live call" {
		t.Fatalf("used the pre-restart plugin: %q", got)
	}
}

func toolLoopPlugin(t *testing.T, beforeRead func()) func(string, string) string {
	t.Helper()
	t.Setenv("FAKE_TOOL_LOOP", "1")
	base := fakeSignedIn(t, "https://fake.invalid/v1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return func(session, body string) string {
		t.Helper()
		res, err := Fetch(ctx, FetchRequest{Provider: "fakeco", Session: session, Model: "fake-1", URL: base + "/chat/completions", Method: "POST", Body: []byte(body)})
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if beforeRead != nil {
			beforeRead()
			beforeRead = nil
		}
		b, err := io.ReadAll(res.Body)
		if err != nil || res.StatusCode != 200 {
			t.Fatalf("fetch: %s, %v, %s", res.Status, err, b)
		}
		// Stand in for the gateway decoder reporting the upstream stop.
		if note, ok := res.Body.(interface{ PluginTurnStop(string) }); ok {
			switch string(b) {
			case "finished":
				note.PluginTurnStop("end_turn")
			case "no live call":
			default:
				note.PluginTurnStop("tool_use")
			}
		}
		return string(b)
	}
}

func TestConfirmedToolWaitOutlastsNormalDrain(t *testing.T) {
	var sessions recentConversations
	key := conversationKey{provider: "agy-cli", session: "A"}
	c := sessions.take(key, time.Now(), true)
	first := &body{id: 1, c: &call{conversation: c}}
	first.PluginTurnStop("tool_use")
	c.finish()
	// No HTTP call is running. An explicit tool_use keeps its owner even
	// for a tool that finishes after the usual heavy-host deadline.
	after := time.Now().Add(15 * time.Minute)
	if !sessions.live(after, true) || sessions.take(key, after, false) != c {
		t.Fatal("long-running tool lost its owner")
	}
	next := &body{id: 2, c: &call{conversation: c}}
	next.PluginTurnStop("end_turn")
	first.PluginTurnStop("tool_use") // delayed observation from the earlier fetch
	c.finish()
	if sessions.live(time.Now(), false) {
		t.Fatal("completed turn still pins its host")
	}
	c = sessions.take(key, time.Now(), true)
	first = &body{id: 3, c: &call{conversation: c}}
	first.PluginTurnStop("tool_use")
	c.finish()
	if sessions.live(time.Now().Add(toolResultWait+time.Second), true) {
		t.Fatal("abandoned tool kept the heavy host forever")
	}
}

func TestSecondRecycleWaitsForToolLoop(t *testing.T) {
	var original *host
	ask := toolLoopPlugin(t, func() { original = currentHost(); original.recycle() })
	a := ask("A", "{}") // recycled after the head, before the tool stop
	b := ask("B", "{}")
	replacement := currentHost()
	replacement.recycle()
	if currentHost() != replacement {
		t.Fatal("started a third host with the first still waiting for a tool")
	}
	if got := ask("A", fmt.Sprintf(`{"result":%q}`, a)); got != "finished" {
		t.Fatalf("first owner lost its call: %s", got)
	}
	select {
	case <-original.dead:
	case <-time.After(10 * time.Second):
		t.Fatal("completed tool loop kept the old host")
	}
	deadline := time.Now().Add(10 * time.Second)
	for currentHost() == replacement {
		if time.Now().After(deadline) {
			t.Fatal("replacement lost its queued heavy report")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := ask("B", fmt.Sprintf(`{"result":%q}`, b)); got != "finished" {
		t.Fatalf("second owner lost its call: %s", got)
	}
}
