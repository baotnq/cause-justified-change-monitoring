package report

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

type alert struct {
	Type  string `json:"type"`
	Actor string `json:"actor"`
}

func writeChain(t *testing.T, alerts ...alert) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, a := range alerts {
		if _, err := w.Append(a); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	return &buf
}

func TestVerifyAcceptsAnIntactChain(t *testing.T) {
	buf := writeChain(t,
		alert{Type: "unauthorized_change", Actor: "u-mallory"},
		alert{Type: "missing_change", Actor: "u-dave"},
	)

	entries, head, err := Verify(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("verified %d entries, want 2", len(entries))
	}
	if entries[0].PrevHash != GenesisHash {
		t.Fatal("first entry does not chain to genesis")
	}
	if entries[1].PrevHash != entries[0].Hash {
		t.Fatal("second entry does not chain to the first")
	}
	if head != entries[1].Hash {
		t.Fatal("head is not the last entry's hash")
	}
}

// The case the chain exists for: the actor named in an alert edits the line
// that names them.
func TestVerifyDetectsAnEditedEntry(t *testing.T) {
	buf := writeChain(t,
		alert{Type: "unauthorized_change", Actor: "u-mallory"},
		alert{Type: "missing_change", Actor: "u-dave"},
	)
	tampered := strings.Replace(buf.String(), "u-mallory", "u-nobody!", 1)

	_, _, err := Verify(strings.NewReader(tampered))
	if err == nil {
		t.Fatal("an edited entry verified")
	}
	var ve *VerificationError
	if !asVerificationError(err, &ve) || ve.Seq != 0 {
		t.Fatalf("error = %v, want a VerificationError at entry 0", err)
	}
}

func TestVerifyDetectsADeletedEntry(t *testing.T) {
	buf := writeChain(t,
		alert{Type: "a", Actor: "u-1"},
		alert{Type: "b", Actor: "u-2"},
		alert{Type: "c", Actor: "u-3"},
	)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	withoutMiddle := lines[0] + "\n" + lines[2] + "\n"

	_, _, err := Verify(strings.NewReader(withoutMiddle))
	if err == nil {
		t.Fatal("a chain with a deleted entry verified")
	}
}

// Truncating the tail is the cheapest attack: drop the last few lines and the
// remaining chain is still internally consistent. Only a head hash held
// somewhere else catches it, which is why Head exists and why the chain alone
// is not enough.
func TestTruncationIsOnlyCaughtByComparingHeads(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	for _, a := range []alert{{Type: "a"}, {Type: "b"}, {Type: "c"}} {
		if _, err := w.Append(a); err != nil {
			t.Fatal(err)
		}
	}
	publishedHead := w.Head()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	truncated := lines[0] + "\n" + lines[1] + "\n"

	entries, head, err := Verify(strings.NewReader(truncated))
	if err != nil {
		t.Fatalf("truncated chain failed internal verification: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("verified %d entries, want 2", len(entries))
	}
	if head == publishedHead {
		t.Fatal("truncation went undetected even against the published head")
	}
}

func TestPayloadSurvivesRoundTrip(t *testing.T) {
	buf := writeChain(t, alert{Type: "unauthorized_change", Actor: "u-mallory"})

	entries, _, err := Verify(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	var got alert
	if err := json.Unmarshal(entries[0].Payload, &got); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if got.Actor != "u-mallory" {
		t.Fatalf("actor = %q, want u-mallory", got.Actor)
	}
}

func TestVerifyRejectsReorderedEntries(t *testing.T) {
	buf := writeChain(t, alert{Type: "a"}, alert{Type: "b"})
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	swapped := lines[1] + "\n" + lines[0] + "\n"

	if _, _, err := Verify(strings.NewReader(swapped)); err == nil {
		t.Fatal("reordered entries verified")
	}
}

func asVerificationError(err error, target **VerificationError) bool {
	ve, ok := err.(*VerificationError)
	if ok {
		*target = ve
	}
	return ok
}
