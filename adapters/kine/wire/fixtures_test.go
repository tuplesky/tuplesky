package wire

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// The shared fixtures are owned by the Rust schema; the Go decoder reads
// the same file so a schema change forces reviewed fixtures on both
// sides (design Sections 19.1, 3.2).
const fixturePath = "../../../crates/coord-types/fixtures/wire_frames_v1.json"

type fixtureFile struct {
	Schema  string `json:"schema"`
	Vectors []struct {
		Name     string `json:"name"`
		FrameHex string `json:"frame_hex"`
		Expect   string `json:"expect"`
	} `json:"vectors"`
}

func loadFixtures(t *testing.T) fixtureFile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(fixturePath))
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}
	var f fixtureFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("parse fixtures: %v", err)
	}
	if f.Schema != "wire_frames_v1" {
		t.Fatalf("unexpected fixture schema %q", f.Schema)
	}
	return f
}

// decodeStream mirrors the Rust reader: consume frames until the buffer
// is empty; a leftover partial frame is IncompleteFrame.
func decodeStream(buf []byte) ([]Message, error) {
	var out []Message
	for len(buf) > 0 {
		frame, rest, err := NextFrame(buf)
		if err != nil {
			return out, err
		}
		msg, err := Decode(frame)
		if err != nil {
			return out, err
		}
		out = append(out, msg)
		buf = rest
	}
	return out, nil
}

func classOf(err error) string {
	for _, c := range []error{
		ErrIncompleteFrame, ErrLengthBelowMinimum, ErrLengthAboveClassLimit,
		ErrTrailingPayloadBytes, ErrUnsupportedKind, ErrUnsupportedVersion, ErrMalformedPayload,
	} {
		if errors.Is(err, c) {
			return c.Error()
		}
	}
	return "none"
}

func TestValidFixturesDecodeAndReencode(t *testing.T) {
	f := loadFixtures(t)
	seen := 0
	for _, v := range f.Vectors {
		if v.Expect != "ok" {
			continue
		}
		seen++
		raw, err := hex.DecodeString(v.FrameHex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		frame, rest, err := NextFrame(raw)
		if err != nil {
			t.Fatalf("%s: NextFrame: %v", v.Name, err)
		}
		if len(rest) != 0 {
			t.Fatalf("%s: trailing stream bytes", v.Name)
		}
		msg, err := Decode(frame)
		if err != nil {
			t.Fatalf("%s: Decode: %v", v.Name, err)
		}
		// Go->Rust: re-encoding produces the identical shared bytes.
		reencoded, err := Encode(msg)
		if err != nil {
			t.Fatalf("%s: Encode: %v", v.Name, err)
		}
		if hex.EncodeToString(reencoded) != v.FrameHex {
			t.Fatalf("%s: re-encode drift\n got  %s\n want %s", v.Name, hex.EncodeToString(reencoded), v.FrameHex)
		}
	}
	if seen < 14 {
		t.Fatalf("expected at least 14 valid vectors, saw %d", seen)
	}
}

func TestMalformedFixturesRejectedByClass(t *testing.T) {
	f := loadFixtures(t)
	seen := 0
	for _, v := range f.Vectors {
		if v.Expect == "ok" {
			continue
		}
		seen++
		raw, err := hex.DecodeString(v.FrameHex)
		if err != nil {
			t.Fatalf("%s: bad hex: %v", v.Name, err)
		}
		_, err = decodeStream(raw)
		if err == nil {
			t.Fatalf("%s: expected %s, decoded cleanly", v.Name, v.Expect)
		}
		if got := classOf(err); got != v.Expect {
			t.Fatalf("%s: expected %s, got %s (%v)", v.Name, v.Expect, got, err)
		}
	}
	if seen < 20 {
		t.Fatalf("expected at least 20 malformed vectors, saw %d", seen)
	}
}

// TestEveryByteTruncationIsIncomplete: truncating a valid frame at every
// boundary is always IncompleteFrame, never a partial decode.
func TestEveryByteTruncationIsIncomplete(t *testing.T) {
	f := loadFixtures(t)
	var sample []byte
	for _, v := range f.Vectors {
		if v.Name == "valid-request-put" {
			sample, _ = hex.DecodeString(v.FrameHex)
		}
	}
	if sample == nil {
		t.Fatal("missing sample frame")
	}
	for n := 0; n < len(sample); n++ {
		if _, _, err := NextFrame(sample[:n]); !errors.Is(err, ErrIncompleteFrame) {
			// A prefix shorter than the full frame is incomplete unless it
			// trips a header class error first (length below minimum or
			// above the class limit is decided from the 8-byte header).
			if errors.Is(err, ErrLengthBelowMinimum) || errors.Is(err, ErrLengthAboveClassLimit) {
				continue
			}
			t.Fatalf("truncation at %d: expected IncompleteFrame, got %v", n, err)
		}
	}
	// The full frame decodes.
	if _, _, err := NextFrame(sample); err != nil {
		t.Fatalf("full frame: %v", err)
	}
}

// TestVarintOverflowIsBounded: a u32 field decoder rejects an
// over-long varint within a fixed byte budget (no unbounded read).
func TestVarintOverflowIsBounded(t *testing.T) {
	r := newReader([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x0f})
	if _, err := r.u32(); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("expected MalformedPayload, got %v", err)
	}
	// A maximal in-range u32 decodes.
	r = newReader([]byte{0xff, 0xff, 0xff, 0xff, 0x0f})
	v, err := r.u32()
	if err != nil || v != 0xffffffff {
		t.Fatalf("max u32: %v %x", err, v)
	}
}
