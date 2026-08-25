// Package report is the evidence trail: an append-only, hash-chained log of
// every alert the monitor produced.
//
// Part A8 asks for evidence that can be replayed. A plain log file does not
// qualify — anyone who can write to it can also rewrite it, and an attacker who
// tripped the monitor has an obvious interest in the line that says so. Chaining
// each entry to the hash of the one before makes a deletion or an edit visible:
// the chain stops verifying at exactly the entry that was touched.
//
// This does not make the log tamper-proof, only tamper-evident, and only for
// whoever checks the chain. Shipping the head hash somewhere the audited system
// cannot reach — a second store, a signed daily digest — is what turns that into
// a guarantee.
package report

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"
)

// GenesisHash starts the chain. An empty PrevHash would let an attacker delete
// the first entries and present the remainder as a complete chain.
const GenesisHash = "0000000000000000000000000000000000000000000000000000000000000000"

// Entry is one line of the report.
type Entry struct {
	Seq      int64           `json:"seq"`
	TS       time.Time       `json:"ts"`
	Payload  json.RawMessage `json:"payload"`
	PrevHash string          `json:"prev_hash"`
	Hash     string          `json:"hash"`
}

// hash binds sequence, timestamp, payload and predecessor together. Leaving any
// of them out leaves room to reorder or replay entries without breaking the
// chain.
func hash(seq int64, ts time.Time, payload []byte, prev string) string {
	h := sha256.New()
	fmt.Fprintf(h, "%d\x00%s\x00", seq, ts.UTC().Format(time.RFC3339Nano))
	h.Write(payload)
	h.Write([]byte{0})
	h.Write([]byte(prev))
	return hex.EncodeToString(h.Sum(nil))
}

// Writer appends entries to an io.Writer as JSON lines.
type Writer struct {
	mu   sync.Mutex
	w    *bufio.Writer
	seq  int64
	prev string
}

// NewWriter starts a fresh chain.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w), prev: GenesisHash}
}

// Append writes one payload and returns the entry as stored.
func (w *Writer) Append(v any) (Entry, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return Entry{}, fmt.Errorf("marshal payload: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	e := Entry{
		Seq:      w.seq,
		TS:       time.Now().UTC(),
		Payload:  payload,
		PrevHash: w.prev,
	}
	e.Hash = hash(e.Seq, e.TS, e.Payload, e.PrevHash)

	line, err := json.Marshal(e)
	if err != nil {
		return Entry{}, fmt.Errorf("marshal entry: %w", err)
	}
	if _, err := w.w.Write(append(line, '\n')); err != nil {
		return Entry{}, fmt.Errorf("write entry: %w", err)
	}
	if err := w.w.Flush(); err != nil {
		return Entry{}, fmt.Errorf("flush: %w", err)
	}

	w.seq++
	w.prev = e.Hash
	return e, nil
}

// Head is the current chain head — the value to publish somewhere the audited
// system cannot write.
func (w *Writer) Head() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.prev
}

// VerificationError says which entry broke the chain and how.
type VerificationError struct {
	Seq    int64
	Reason string
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("report entry %d: %s", e.Seq, e.Reason)
}

// Verify replays a report and checks every link. It returns the entries it
// verified and the chain head, or the first entry that failed.
//
// Replay is the point: an auditor does not have to trust the process that wrote
// the file, only the arithmetic.
func Verify(r io.Reader) ([]Entry, string, error) {
	var (
		entries []Entry
		prev    = GenesisHash
		want    int64
	)

	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			return entries, prev, &VerificationError{Seq: want, Reason: "malformed JSON: " + err.Error()}
		}
		if e.Seq != want {
			return entries, prev, &VerificationError{
				Seq:    e.Seq,
				Reason: fmt.Sprintf("out of sequence: expected %d — an entry was removed or reordered", want),
			}
		}
		if e.PrevHash != prev {
			return entries, prev, &VerificationError{
				Seq:    e.Seq,
				Reason: fmt.Sprintf("does not chain: prev_hash %s, expected %s", short(e.PrevHash), short(prev)),
			}
		}
		if got := hash(e.Seq, e.TS, e.Payload, e.PrevHash); got != e.Hash {
			return entries, prev, &VerificationError{
				Seq:    e.Seq,
				Reason: fmt.Sprintf("content does not match its hash: recomputed %s, stored %s", short(got), short(e.Hash)),
			}
		}
		entries = append(entries, e)
		prev = e.Hash
		want++
	}
	if err := sc.Err(); err != nil {
		return entries, prev, fmt.Errorf("read report: %w", err)
	}
	return entries, prev, nil
}

func short(h string) string {
	if len(h) <= 12 {
		return h
	}
	return h[:12] + "…"
}

// Encode renders entries back to the JSON-lines form Verify reads, so a chain
// that was stored elsewhere — a database table, an object store — can be
// verified by exactly the same code that verifies the file.
func Encode(entries []Entry) (io.Reader, error) {
	var buf bytes.Buffer
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("marshal entry %d: %w", e.Seq, err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return &buf, nil
}
