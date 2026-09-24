package wire

import (
	"errors"
	"testing"
)

// rawFrame encodes a DTO without its schema checks, standing in for a
// sender that trusts the frame class limit alone.
func rawFrame(t *testing.T, m Message) []byte {
	t.Helper()
	var w writer
	m.encode(&w)
	frame, err := EncodeFrame(m.kind(), 1, w.buf)
	if err != nil {
		t.Fatalf("raw frame: %v", err)
	}
	return frame
}

// TestEncodeRefusesWhatDecodeRefuses builds each out-of-schema DTO
// directly, checks that the decoder rejects its unchecked encoding, and
// then that Encode refuses the same value before it is written.
func TestEncodeRefusesWhatDecodeRefuses(t *testing.T) {
	over := func(n int) []byte { return make([]byte, n+1) }
	bigKey := over(maxKeyBytes)
	bigValue := over(maxValueBytes)
	cases := []struct {
		name string
		msg  Message
	}{
		{"hello role", Hello{Role: RoleLearner + 1}},
		{"hello capabilities", Hello{Capabilities: make([]uint16, maxCapabilities+1)}},
		{"hello-ack capabilities", HelloAck{Capabilities: make([]uint16, maxCapabilities+1)}},
		{"close reason", Close{Reason: over(maxReasonBytes)}},
		{"request logical", Request{Logical: over(maxRequestBytes)}},
		{"response tag", Response{Tag: OutcomeUnknown + 1}},
		{"response detail", Response{Tag: OutcomeErr, Detail: over(maxReasonBytes)}},
		{"watch-open key", WatchOpen{Key: bigKey}},
		{"watch-open range end", WatchOpen{RangeEnd: &bigKey}},
		{"watch-events count", WatchEvents{Events: make([]Event, maxEventsPerBatch+1)}},
		{"watch-events kind", WatchEvents{Events: []Event{{Kind: EventDelete + 1}}}},
		{"watch-events key", WatchEvents{Events: []Event{{Key: bigKey}}}},
		{"watch-events value", WatchEvents{Events: []Event{{Value: bigValue}}}},
		{"watch-events prev value", WatchEvents{Events: []Event{{PrevValue: &bigValue}}}},
		{"watch-close reason", WatchClose{Reason: WatchSourceLost + 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame, _, err := NextFrame(rawFrame(t, tc.msg))
			if err != nil {
				t.Fatalf("next frame: %v", err)
			}
			if _, err := Decode(frame); err == nil {
				t.Fatalf("decoder accepted the unchecked encoding")
			}
			if _, err := Encode(tc.msg); !errors.Is(err, ErrMalformedPayload) {
				t.Fatalf("Encode = %v, want MalformedPayload", err)
			}
		})
	}
}

// TestEncodeAcceptsValuesAtTheirBounds keeps the checks from refusing
// the largest values the schema allows.
func TestEncodeAcceptsValuesAtTheirBounds(t *testing.T) {
	key := make([]byte, maxKeyBytes)
	value := make([]byte, maxValueBytes)
	msgs := []Message{
		Hello{Role: RoleLearner, Capabilities: make([]uint16, maxCapabilities)},
		Close{Reason: make([]byte, maxReasonBytes)},
		Response{Tag: OutcomeErr, Detail: make([]byte, maxReasonBytes)},
		WatchOpen{Key: key, RangeEnd: &key},
		WatchEvents{Events: []Event{{Kind: EventDelete, Key: key, Value: value, PrevValue: &value}}},
		WatchClose{Reason: WatchSourceLost},
	}
	for _, m := range msgs {
		encoded, err := Encode(m)
		if err != nil {
			t.Fatalf("Encode(%T) = %v", m, err)
		}
		frame, _, err := NextFrame(encoded)
		if err != nil {
			t.Fatalf("next frame: %v", err)
		}
		if _, err := Decode(frame); err != nil {
			t.Fatalf("Decode(%T) = %v", m, err)
		}
	}
}
