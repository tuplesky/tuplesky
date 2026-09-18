package client

import (
	"context"
	"encoding/binary"
	"io"

	quic "github.com/quic-go/quic-go"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
)

const maxFrameBytes = 8*1024*1024 + 64*1024

// writeFrame writes a complete framed message.
func writeFrame(stream *quic.Stream, frame []byte) error {
	_, err := stream.Write(frame)
	return err
}

// readOneFrame reads exactly one wire frame from a stream, honouring the
// stream context deadline.
func readOneFrame(ctx context.Context, stream *quic.Stream) (wire.Frame, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = stream.SetReadDeadline(deadline)
	}
	header := make([]byte, 8)
	if _, err := io.ReadFull(stream, header); err != nil {
		return wire.Frame{}, err
	}
	length := binary.BigEndian.Uint32(header[0:4])
	if length < 4 || uint64(length) > maxFrameBytes {
		return wire.Frame{}, wire.ErrMalformedPayload
	}
	payload := make([]byte, int(length)-4)
	if _, err := io.ReadFull(stream, payload); err != nil {
		return wire.Frame{}, err
	}
	full := make([]byte, 0, 8+len(payload))
	full = append(full, header...)
	full = append(full, payload...)
	frame, rest, err := wire.NextFrame(full)
	if err != nil {
		return wire.Frame{}, err
	}
	if len(rest) != 0 {
		return wire.Frame{}, wire.ErrTrailingPayloadBytes
	}
	return frame, nil
}
