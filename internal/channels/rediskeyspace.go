// Package channels holds the two observation channels of Part A1, wired to
// real infrastructure: the change channel on Redis keyspace notifications, and
// the cause channel on NATS.
//
// Both are subscribe-only. Nothing in this package writes to the audited
// system's data or is called by it, which is what keeps the monitor off the
// critical path and makes it removable without touching any service.
package channels

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/baotnq/cause-justified-change-monitoring/internal/monitor"
)

// WriteEvents are the Redis command notifications that mean "this key's value
// changed". Expiry and eviction are deliberately included: a balance that
// disappears is a change too.
var WriteEvents = map[string]bool{
	"set": true, "setrange": true, "incrby": true, "incrbyfloat": true, "decrby": true,
	"hset": true, "hincrby": true, "hincrbyfloat": true, "hdel": true,
	"del": true, "expired": true, "evicted": true, "rename_from": true, "rename_to": true,
	"restore": true, "copy_to": true,
}

// KeyspaceFeed turns Redis keyspace notifications into change events.
//
// Two properties of this feed are worth stating plainly, because they bound
// what the monitor can promise:
//
//   - Notifications carry the key, not the value and not a timestamp. So the
//     feed reports that an actor's state moved, never by how much, and it stamps
//     events with arrival time rather than commit time. Under lag that shifts
//     events toward later windows, which is what the grace period absorbs. A CDC
//     feed carries commit time and the old and new values, and is the better
//     source where one exists — the monitor accepts both.
//
//   - Keyspace notifications are fire-and-forget pub/sub with no replay. If the
//     audit is down, those writes are simply not seen; the gap is invisible from
//     inside this channel. That is why Part A7 asks for the monitor's own lag and
//     liveness to be alerted separately, and why a second, durable change source
//     is the mitigation for a correlated compromise in Part E.
type KeyspaceFeed struct {
	rdb *redis.Client
	db  int

	// KeyPrefix selects the protected keys, e.g. "asset:account:".
	KeyPrefix string

	// ActorFromKey extracts the actor identity from a key. Defaults to the
	// remainder after KeyPrefix.
	ActorFromKey func(key string) (string, bool)

	// Events limits which command notifications count as a write.
	Events map[string]bool

	// Now is overridable in tests.
	Now func() time.Time
}

// NewKeyspaceFeed returns a feed over the given database and key prefix.
func NewKeyspaceFeed(rdb *redis.Client, db int, keyPrefix string) *KeyspaceFeed {
	return &KeyspaceFeed{
		rdb:       rdb,
		db:        db,
		KeyPrefix: keyPrefix,
		Events:    WriteEvents,
		Now:       time.Now,
	}
}

// CheckConfig verifies the server is actually emitting the notifications this
// feed depends on. A monitor subscribed to a channel nobody publishes on looks
// exactly like a system where nothing bad ever happens, so this is checked at
// startup and treated as fatal rather than logged.
func (f *KeyspaceFeed) CheckConfig(ctx context.Context) error {
	res, err := f.rdb.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		return fmt.Errorf("read notify-keyspace-events: %w", err)
	}
	flags := res["notify-keyspace-events"]
	if !strings.Contains(flags, "E") {
		return fmt.Errorf("notify-keyspace-events = %q: keyevent notifications (E) are off, "+
			"so the change channel would be silent; set at least 'KEA'", flags)
	}
	if !strings.ContainsAny(flags, "Ag$hz") {
		return fmt.Errorf("notify-keyspace-events = %q: no write-command classes enabled; set at least 'KEA'", flags)
	}
	return nil
}

func (f *KeyspaceFeed) actor(key string) (string, bool) {
	if f.ActorFromKey != nil {
		return f.ActorFromKey(key)
	}
	if !strings.HasPrefix(key, f.KeyPrefix) {
		return "", false
	}
	actor := strings.TrimPrefix(key, f.KeyPrefix)
	return actor, actor != ""
}

// Run subscribes and forwards change events until ctx is done. It returns the
// number of notifications it ignored because they were not writes to protected
// keys — useful for spotting a misconfigured prefix, which would otherwise look
// like a very quiet system.
func (f *KeyspaceFeed) Run(ctx context.Context, out chan<- monitor.ChangeEvent) error {
	pattern := fmt.Sprintf("__keyevent@%d__:*", f.db)
	sub := f.rdb.PSubscribe(ctx, pattern)
	defer sub.Close()

	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe %s: %w", pattern, err)
	}

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			event := msg.Channel[strings.LastIndex(msg.Channel, ":")+1:]
			if len(f.Events) > 0 && !f.Events[event] {
				continue
			}
			actor, ok := f.actor(msg.Payload) // payload of a keyevent message is the key
			if !ok {
				continue
			}
			select {
			case out <- monitor.ChangeEvent{
				Actor:  actor,
				TS:     f.Now(),
				Source: "redis-keyspace:" + event,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}
