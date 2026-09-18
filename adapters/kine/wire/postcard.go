package wire

import "errors"

// reader consumes a postcard payload with checked bounds.
type reader struct {
	buf []byte
	pos int
}

func newReader(buf []byte) *reader { return &reader{buf: buf} }

func (r *reader) remaining() int { return len(r.buf) - r.pos }

// done reports whether every payload byte was consumed. A postcard DTO
// that leaves trailing bytes is a framing error.
func (r *reader) done() bool { return r.pos == len(r.buf) }

func (r *reader) byte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, ErrMalformedPayload
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) bytesN(n int) ([]byte, error) {
	if n < 0 || r.pos+n > len(r.buf) {
		return nil, ErrMalformedPayload
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

// varint decodes a postcard unsigned LEB128 varint into a u64, rejecting
// more than maxBytes continuation bytes (overflow for the target width).
func (r *reader) varint(maxBytes int) (uint64, error) {
	var value uint64
	var shift uint
	for i := 0; i < maxBytes; i++ {
		b, err := r.byte()
		if err != nil {
			return 0, err
		}
		if i == maxBytes-1 {
			// The final byte must not carry a continuation bit and must
			// fit the remaining bits.
			if b&0x80 != 0 {
				return 0, ErrMalformedPayload
			}
		}
		value |= uint64(b&0x7f) << shift
		if b&0x80 == 0 {
			return value, nil
		}
		shift += 7
	}
	return 0, ErrMalformedPayload
}

func (r *reader) u16() (uint16, error) {
	v, err := r.varint(3) // ceil(16/7) = 3 bytes
	if err != nil {
		return 0, err
	}
	if v > 0xffff {
		return 0, ErrMalformedPayload
	}
	return uint16(v), nil
}

func (r *reader) u32() (uint32, error) {
	v, err := r.varint(5) // ceil(32/7) = 5 bytes
	if err != nil {
		return 0, err
	}
	if v > 0xffffffff {
		return 0, ErrMalformedPayload
	}
	return uint32(v), nil
}

func (r *reader) u64() (uint64, error) {
	return r.varint(10) // ceil(64/7) = 10 bytes
}

// length decodes a collection or byte length varint (usize) and checks it
// against a bound and the remaining bytes.
func (r *reader) length(bound int) (int, error) {
	v, err := r.varint(10)
	if err != nil {
		return 0, err
	}
	if v > uint64(bound) {
		return 0, ErrMalformedPayload
	}
	return int(v), nil
}

func (r *reader) boolean() (bool, error) {
	b, err := r.byte()
	if err != nil {
		return false, err
	}
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, ErrMalformedPayload
	}
}

func (r *reader) option() (bool, error) { return r.boolean() }

// boundedBytes decodes a length-prefixed byte string bounded by max.
func (r *reader) boundedBytes(max int) ([]byte, error) {
	n, err := r.length(max)
	if err != nil {
		return nil, err
	}
	raw, err := r.bytesN(n)
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

// writer builds a postcard payload.
type writer struct {
	buf []byte
}

func (w *writer) byte(b byte)  { w.buf = append(w.buf, b) }
func (w *writer) raw(b []byte) { w.buf = append(w.buf, b...) }
func (w *writer) boolean(v bool) {
	if v {
		w.byte(1)
	} else {
		w.byte(0)
	}
}

func (w *writer) varint(v uint64) {
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			w.byte(b | 0x80)
		} else {
			w.byte(b)
			return
		}
	}
}

func (w *writer) boundedBytes(b []byte) {
	w.varint(uint64(len(b)))
	w.raw(b)
}

var errShort = errors.New("short")
