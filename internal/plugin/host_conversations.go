package plugin

import (
	"sync"
	"time"
)

// Only automatic memory recycling keeps conversation affinity. A settings
// change or explicit restart must take effect on the very next request.
// When a second host gets heavy while the first still owns a live
// session, queue its recycle: at most one old host takes follow-ups. Both
// pointers are protected by hostMu.
var recycledHost, nextRecycle *host

// releaseOwnerLocked schedules a heavy report the replacement has already
// sent (host.js reports once). recycle rechecks that it is still current.
func releaseOwnerLocked(h *host) {
	if recycledHost != h {
		return
	}
	recycledHost = nil
	if nextRecycle != nil {
		next := nextRecycle
		nextRecycle = nil
		go next.recycle()
	}
}

type conversationKey struct{ provider, account, session, subagent string }

func (r FetchRequest) conversationKey() conversationKey {
	session := r.Headers["x-magpie-session"]
	if session == "" {
		session = r.Session
	}
	return conversationKey{r.Provider, r.Account, session, r.Headers["x-magpie-subagent"]}
}

// Keep only recent conversations. A quiet HTTP connection doesn't tell us
// whether the client is running a tool or has finished the turn.
const conversationIdle = 2 * time.Minute

// agy-cli waits up to thirty minutes for a client-side tool by default.
// Only an explicit upstream tool stop earns that longer wait.
const toolResultWait = 30 * time.Minute

// agy-cli keeps the ACP process and its conversation between user turns
// for ten idle minutes by default (AGY_BRIDGE_IDLE_MS).
const agyTurnIdle = 10 * time.Minute
const maxHostConversations = 512

type recentConversations struct {
	mu      sync.Mutex
	entries map[conversationKey]*hostConversation
}
type hostConversation struct {
	book       *recentConversations
	active     int
	last       time.Time
	reported   int64 // newest fetch whose upstream stop we have seen
	stop       string
	agy        bool
	reportedAt time.Time
}

func (c *hostConversation) holdsSession(now time.Time) bool {
	switch c.stop {
	case "end_turn":
		return c.agy && now.Sub(c.reportedAt) < agyTurnIdle
	case "tool_use":
		return now.Sub(c.reportedAt) < toolResultWait
	}
	return false
}

func (c *hostConversation) live(now time.Time) bool {
	return c.active > 0 || c.holdsSession(now) ||
		c.reported == 0 && now.Sub(c.last) < conversationIdle
}

func (book *recentConversations) take(key conversationKey, now time.Time, create bool) *hostConversation {
	book.mu.Lock()
	defer book.mu.Unlock()
	c := book.entries[key]
	if c == nil || !c.live(now) {
		if !create {
			return nil
		}
		if c == nil && len(book.entries) >= maxHostConversations {
			var oldest *hostConversation
			var evict conversationKey
			for k, v := range book.entries {
				if v.active == 0 && !(v.stop == "tool_use" && v.live(now)) && (oldest == nil || v.last.Before(oldest.last)) {
					oldest, evict = v, k
				}
			}
			if oldest == nil {
				return nil
			}
			delete(book.entries, evict)
		}
		if book.entries == nil {
			book.entries = make(map[conversationKey]*hostConversation)
		}
		c = &hostConversation{book: book, agy: key.provider == "agy-cli"}
		book.entries[key] = c
	}
	c.active++
	return c
}

func (c *hostConversation) finish() {
	if c == nil {
		return
	}
	c.book.mu.Lock()
	c.active--
	c.last = time.Now()
	c.book.mu.Unlock()
}

func (book *recentConversations) live(now time.Time, pastDrainLimit bool) bool {
	book.mu.Lock()
	defer book.mu.Unlock()
	for _, c := range book.entries {
		if pastDrainLimit {
			// Across the ordinary drain deadline, retain only a known session
			// or its bounded in-flight continuation.
			if c.holdsSession(now) || c.reported != 0 && c.active > 0 && now.Sub(c.reportedAt) < toolResultWait {
				return true
			}
		} else if c.live(now) {
			return true
		}
	}
	return false
}

// PluginTurnStop receives the raw Anthropic stop the gateway already read.
// A late observation from a parallel or retried request cannot reopen a
// turn a newer upstream response has ended.
func (b *body) PluginTurnStop(reason string) {
	c := b.c.conversation
	if c == nil {
		return
	}
	c.book.mu.Lock()
	defer c.book.mu.Unlock()
	if b.id >= c.reported {
		c.reported = b.id
		c.stop = reason
		c.reportedAt = time.Now()
	}
}
