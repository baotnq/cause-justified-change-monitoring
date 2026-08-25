package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/baotnq/cause-justified-change-monitoring/internal/monitor"
)

// CauseMessage is the wire format of the cause channel.
//
// Producer is carried in the message, but the monitor must not take it on
// trust: a field an attacker can set is not an identity. In a real deployment
// it is bound to the connection — a NATS account or user per service, or a
// signature over the payload — and the field is only what the monitor records.
// The allow-list in monitor.Config is the enforcement point either way, and it
// is the reason scenario 6's agent cannot vouch for itself.
type CauseMessage struct {
	Actor    string    `json:"actor"`
	TS       time.Time `json:"ts"`
	Kind     string    `json:"kind"`
	Producer string    `json:"producer"`
	Amount   int64     `json:"amount,omitempty"`
	Ref      string    `json:"ref,omitempty"`
}

// CauseBus subscribes to authorized business events.
type CauseBus struct {
	nc      *nats.Conn
	subject string

	// Now supplies a timestamp for messages that arrive without one. Events
	// should carry their own; a missing timestamp means the producer is
	// misconfigured, and arrival time is the least-wrong fallback.
	Now func() time.Time

	// OnMalformed is called for messages that cannot be parsed. They are not
	// dropped quietly: a producer publishing garbage is a producer whose events
	// are not justifying anything, which will surface later as violations that
	// nobody can explain.
	OnMalformed func(subject string, data []byte, err error)
}

// NewCauseBus subscribes to a subject such as "cause.>".
func NewCauseBus(nc *nats.Conn, subject string) *CauseBus {
	return &CauseBus{nc: nc, subject: subject, Now: time.Now}
}

// Run forwards cause events until ctx is done.
func (b *CauseBus) Run(ctx context.Context, out chan<- monitor.CauseEvent) error {
	msgs := make(chan *nats.Msg, 1024)
	sub, err := b.nc.ChanSubscribe(b.subject, msgs)
	if err != nil {
		return fmt.Errorf("subscribe %s: %w", b.subject, err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-msgs:
			var m CauseMessage
			if err := json.Unmarshal(msg.Data, &m); err != nil {
				if b.OnMalformed != nil {
					b.OnMalformed(msg.Subject, msg.Data, err)
				}
				continue
			}
			if m.Actor == "" {
				if b.OnMalformed != nil {
					b.OnMalformed(msg.Subject, msg.Data, fmt.Errorf("no actor"))
				}
				continue
			}
			ts := m.TS
			if ts.IsZero() {
				ts = b.Now()
			}
			select {
			case out <- monitor.CauseEvent{
				Actor:    m.Actor,
				TS:       ts,
				Kind:     m.Kind,
				Producer: m.Producer,
				Amount:   m.Amount,
				Ref:      m.Ref,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// Publish is a helper for the scenario driver and for tests. Real producers are
// the services themselves.
func Publish(nc *nats.Conn, subject string, m CauseMessage) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return nc.Publish(subject, data)
}
