// Package wire is the restricted postcard subset and shared frame codec
// of the Kine adapter (task-44; design Sections 3.2, 6.6, 19.1). The
// numeric kinds, versions and byte layout are owned by the Rust wire_v1
// schema (task-03); this package mirrors only the frames a trusted Go
// collector must read and write, verified against the shared fixtures.
// No Serde reflection, cgo, protobuf-on-wire or Go voting state machine
// is introduced here.
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// SchemaFamily names the frozen frame family this package decodes.
const SchemaFamily = "wire_v1"

// headerLen is the fixed frame header: u32_be length, u16_be kind,
// u16_be version. The length excludes itself and includes kind+version.
const headerLen = 8

// minFrameLength is the smallest length field: kind+version, empty
// payload.
const minFrameLength = 4

// Frame error classes mirror the Rust wire_v1 WireError variants that the
// shared fixtures name.
var (
	// ErrIncompleteFrame: the stream ended before a full frame.
	ErrIncompleteFrame = errors.New("IncompleteFrame")
	// ErrLengthBelowMinimum: the length field is below kind+version.
	ErrLengthBelowMinimum = errors.New("LengthBelowMinimum")
	// ErrLengthAboveClassLimit: the length exceeds the kind's class limit.
	ErrLengthAboveClassLimit = errors.New("LengthAboveClassLimit")
	// ErrTrailingPayloadBytes: the DTO decoded but left payload bytes.
	ErrTrailingPayloadBytes = errors.New("TrailingPayloadBytes")
	// ErrUnsupportedKind: the kind is not registered.
	ErrUnsupportedKind = errors.New("UnsupportedKind")
	// ErrUnsupportedVersion: the version is not supported for the kind.
	ErrUnsupportedVersion = errors.New("UnsupportedVersion")
	// ErrMalformedPayload: the payload did not decode as its DTO.
	ErrMalformedPayload = errors.New("MalformedPayload")
)

// Kind is a registered wire_v1 message kind.
type Kind uint16

// The registered kinds (append-only; owned by the Rust schema).
const (
	KindHello          Kind = 0x0001
	KindHelloAck       Kind = 0x0002
	KindClose          Kind = 0x0003
	KindRequest        Kind = 0x0100
	KindResponse       Kind = 0x0101
	KindResolveRequest Kind = 0x0102
	KindWatchOpen      Kind = 0x0200
	KindWatchEvents    Kind = 0x0201
	KindWatchProgress  Kind = 0x0202
	KindWatchClose     Kind = 0x0203
)

// kindClass returns the class byte (high byte) of a kind.
func kindClass(kind uint16) (uint16, bool) {
	switch kind >> 8 {
	case 0x00, 0x01, 0x02:
		return kind >> 8, true
	default:
		// Ranges 0x03+ are reserved for other planes (protocol evidence,
		// configuration, snapshots, collector evidence): not client frames.
		return kind >> 8, false
	}
}

// maxFrameLength is the class limit for a kind's range.
func maxFrameLength(kind uint16) uint32 {
	switch kind >> 8 {
	case 0x00:
		return 64 * 1024
	case 0x01:
		return 3 * 1024 * 1024
	case 0x02:
		return 8*1024*1024 + 64*1024
	default:
		return 64 * 1024
	}
}

func registered(kind uint16) bool {
	switch Kind(kind) {
	case KindHello, KindHelloAck, KindClose, KindRequest, KindResponse,
		KindResolveRequest, KindWatchOpen, KindWatchEvents, KindWatchProgress, KindWatchClose:
		return true
	default:
		return false
	}
}

// Frame is a decoded frame header plus its raw payload.
type Frame struct {
	Kind    uint16
	Version uint16
	Payload []byte
}

// NextFrame reads one frame from the front of buf and returns it with the
// remaining bytes. It classifies header errors exactly as the Rust
// reader does before any payload is inspected.
func NextFrame(buf []byte) (Frame, []byte, error) {
	if len(buf) < headerLen {
		return Frame{}, buf, ErrIncompleteFrame
	}
	length := binary.BigEndian.Uint32(buf[0:4])
	kind := binary.BigEndian.Uint16(buf[4:6])
	version := binary.BigEndian.Uint16(buf[6:8])
	if length < minFrameLength {
		return Frame{}, buf, ErrLengthBelowMinimum
	}
	if length > maxFrameLength(kind) {
		return Frame{}, buf, ErrLengthAboveClassLimit
	}
	total := headerLen + int(length) - 4 // length includes the 4 kind+version bytes
	if len(buf) < total {
		return Frame{}, buf, ErrIncompleteFrame
	}
	payload := make([]byte, total-headerLen)
	copy(payload, buf[headerLen:total])
	return Frame{Kind: kind, Version: version, Payload: payload}, buf[total:], nil
}

// EncodeFrame writes a frame header and payload for a registered kind.
func EncodeFrame(kind uint16, version uint16, payload []byte) ([]byte, error) {
	length := uint64(len(payload)) + 4
	if length > uint64(maxFrameLength(kind)) {
		return nil, ErrLengthAboveClassLimit
	}
	out := make([]byte, headerLen+len(payload))
	binary.BigEndian.PutUint32(out[0:4], uint32(length))
	binary.BigEndian.PutUint16(out[4:6], kind)
	binary.BigEndian.PutUint16(out[6:8], version)
	copy(out[headerLen:], payload)
	return out, nil
}

// DecodeError wraps a class error with context, so tests can compare the
// class while logs keep detail.
func decodeErr(class error, detail string) error {
	return fmt.Errorf("%w: %s", class, detail)
}
