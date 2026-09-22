package wire

import (
	"encoding/binary"
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

// TestU64VarintTenthByte: the tenth byte of a u64 varint holds only the
// 64th bit, so any value above 0x01 there is malformed, exactly as
// postcard's DeserializeBadVarint; the overflow bits are never shifted
// away into an aliased value.
func TestU64VarintTenthByte(t *testing.T) {
	nine := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80}
	cases := []struct {
		name  string
		tenth byte
		want  uint64
		ok    bool
	}{
		{name: "zero", tenth: 0x00, want: 0, ok: true},
		{name: "top-bit", tenth: 0x01, want: 1 << 63, ok: true},
		{name: "one-bit-above", tenth: 0x02},
		{name: "two-bits", tenth: 0x03},
		{name: "all-payload-bits", tenth: 0x7f},
		{name: "continuation", tenth: 0x81},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := newReader(append(append([]byte{}, nine...), c.tenth))
			v, err := r.u64()
			if !c.ok {
				if !errors.Is(err, ErrMalformedPayload) {
					t.Fatalf("expected MalformedPayload, got %v (value %#x)", err, v)
				}
				return
			}
			if err != nil || v != c.want {
				t.Fatalf("expected %#x, got %#x (%v)", c.want, v, err)
			}
		})
	}
	// The same nine bytes and a tenth 0x02 as a length prefix are
	// malformed too, never a zero length.
	r := newReader(append(append([]byte{}, nine...), 0x02))
	if _, err := r.length(16); !errors.Is(err, ErrMalformedPayload) {
		t.Fatalf("length: expected MalformedPayload, got %v", err)
	}
}

// TestReservedRangeClassLimits: a header of a reserved range is judged
// against that range's own limit, so a legal frame of another plane is
// not refused as LengthAboveClassLimit before Decode names it
// unsupported; a kind outside every range takes the smallest limit.
func TestReservedRangeClassLimits(t *testing.T) {
	header := func(length uint32, kind uint16) []byte {
		var h [headerLen]byte
		binary.BigEndian.PutUint32(h[0:4], length)
		binary.BigEndian.PutUint16(h[4:6], kind)
		binary.BigEndian.PutUint16(h[6:8], 1)
		return h[:]
	}
	cases := []struct {
		kind  uint16
		limit uint32
	}{
		{kind: 0x0300, limit: 4 * 1024 * 1024},
		{kind: 0x0400, limit: 256 * 1024},
		{kind: 0x0500, limit: 8*1024*1024 + 64*1024},
		{kind: 0x0600, limit: 1024*1024 + 64*1024},
		{kind: 0x0700, limit: 8*1024*1024 + 64*1024},
		{kind: 0x0800, limit: 64 * 1024},
		{kind: 0x0900, limit: 64 * 1024},
		{kind: 0xffff, limit: 64 * 1024},
	}
	for _, c := range cases {
		// At the limit the header is legal and the stream is merely short.
		if _, _, err := NextFrame(header(c.limit, c.kind)); !errors.Is(err, ErrIncompleteFrame) {
			t.Fatalf("kind %#04x at limit: expected IncompleteFrame, got %v", c.kind, err)
		}
		if _, _, err := NextFrame(header(c.limit+1, c.kind)); !errors.Is(err, ErrLengthAboveClassLimit) {
			t.Fatalf("kind %#04x above limit: expected LengthAboveClassLimit, got %v", c.kind, err)
		}
	}
	// A complete reserved-range frame passes the header and is then
	// refused by kind, not by length.
	frame, _, err := NextFrame(append(header(5, 0x0400), 0x00))
	if err != nil {
		t.Fatalf("reserved frame header: %v", err)
	}
	if _, err := Decode(frame); !errors.Is(err, ErrUnsupportedKind) {
		t.Fatalf("reserved frame: expected UnsupportedKind, got %v", err)
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
