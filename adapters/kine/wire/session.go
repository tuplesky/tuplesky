package wire

import "errors"

// The session binding frames of `coord-session` (registered in
// `spec/wire-v1.md` as raw kinds in the API range): `Bind` carries the
// STS service token once per connection, `BindAck` answers with what was
// bound. They are dispatched by raw kind at the frontend boundary, so
// they are not part of the typed [`Decode`] registry.

// The binding kinds.
const (
	KindBind    Kind = 0x0105
	KindBindAck Kind = 0x0106
)

// MaxTokenBytes bounds a service token.
const MaxTokenBytes = 8 * 1024

// ErrUnexpectedFrame: not the binding frame expected here.
var ErrUnexpectedFrame = errors.New("unexpected frame")

// Bind is the binding request.
type Bind struct {
	Token []byte
}

// BindAck is the binding acknowledgement.
type BindAck struct {
	Session        [idBytes]byte
	ExpiresAt      uint64
	Scope          uint32
	RuleGeneration uint64
}

// EncodeBind frames a binding request.
func EncodeBind(b Bind) ([]byte, error) {
	if len(b.Token) > MaxTokenBytes {
		return nil, ErrMalformedPayload
	}
	w := &writer{}
	w.boundedBytes(b.Token)
	return EncodeFrame(uint16(KindBind), 1, w.buf)
}

// DecodeBind parses a binding request frame exactly.
func DecodeBind(frame Frame) (Bind, error) {
	if frame.Kind != uint16(KindBind) || frame.Version != 1 {
		return Bind{}, ErrUnexpectedFrame
	}
	r := newReader(frame.Payload)
	token, err := r.boundedBytes(MaxTokenBytes)
	if err != nil {
		return Bind{}, err
	}
	if !r.done() {
		return Bind{}, ErrTrailingPayloadBytes
	}
	return Bind{Token: token}, nil
}

// EncodeBindAck frames an acknowledgement.
func EncodeBindAck(a BindAck) ([]byte, error) {
	w := &writer{}
	w.raw(a.Session[:])
	w.varint(a.ExpiresAt)
	w.varint(uint64(a.Scope))
	w.varint(a.RuleGeneration)
	return EncodeFrame(uint16(KindBindAck), 1, w.buf)
}

// DecodeBindAck parses an acknowledgement frame exactly.
func DecodeBindAck(frame Frame) (BindAck, error) {
	if frame.Kind != uint16(KindBindAck) || frame.Version != 1 {
		return BindAck{}, ErrUnexpectedFrame
	}
	r := newReader(frame.Payload)
	var a BindAck
	var err error
	if a.Session, err = readID(r); err != nil {
		return BindAck{}, err
	}
	if a.ExpiresAt, err = r.u64(); err != nil {
		return BindAck{}, err
	}
	if a.Scope, err = r.u32(); err != nil {
		return BindAck{}, err
	}
	if a.RuleGeneration, err = r.u64(); err != nil {
		return BindAck{}, err
	}
	if !r.done() {
		return BindAck{}, ErrTrailingPayloadBytes
	}
	return a, nil
}
