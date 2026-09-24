package backend

import (
	"context"
	"errors"

	"github.com/tuplesky/tuplesky/adapters/kine/client"
	"github.com/tuplesky/tuplesky/adapters/kine/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The frozen wire error codes (`coord_types::wire_v1::codes`).
const (
	codeRequestIdentityConflict uint16 = 0x0001
	codeBackpressure            uint16 = 0x0002
	codeMalformedRequest        uint16 = 0x0003
	codeNotAdmitted             uint16 = 0x0004
	codeResultTooLarge          uint16 = 0x0005
)

// mapWireError turns an established error response into the gRPC status
// the API server sees. The detail is the frontend's bounded, redacted
// text.
func mapWireError(code uint16, detail []byte) error {
	msg := string(detail)
	switch code {
	case codeMalformedRequest:
		return status.Error(codes.InvalidArgument, "frontend refused the request: "+msg)
	case codeNotAdmitted:
		return status.Error(codes.PermissionDenied, "not admitted: "+msg)
	case codeBackpressure:
		return status.Error(codes.Unavailable, "frontend backpressure: "+msg)
	case codeResultTooLarge:
		return status.Error(codes.ResourceExhausted, "result too large: "+msg)
	case codeRequestIdentityConflict:
		return status.Error(codes.Internal, "request identity conflict: "+msg)
	default:
		return status.Errorf(codes.Unknown, "frontend error %#04x: %s", code, msg)
	}
}

// mapClientError turns a transport-level client error into a status.
func mapClientError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, client.ErrPoolExhausted):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, client.ErrNotConnected):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, client.ErrBindRejected):
		return status.Error(codes.Unauthenticated, err.Error())
	case errors.Is(err, client.ErrProtocol):
		return status.Error(codes.Internal, err.Error())
	default:
		return status.Error(codes.Unavailable, err.Error())
	}
}

// mapOutcomeError turns an established non-success outcome that the
// calling operation cannot represent into a status.
func mapOutcomeError(kind wire.OutcomeKind) error {
	switch kind {
	case wire.OutcomeErrPermissionDenied:
		return status.Error(codes.PermissionDenied, "current policy does not permit the operation")
	case wire.OutcomeErrSessionInvalid:
		return status.Error(codes.Unauthenticated, "the session cannot execute")
	case wire.OutcomeErrLeaseExists:
		return status.Error(codes.Internal, "binding identity already spent")
	case wire.OutcomeErrCompacted:
		return status.Error(codes.OutOfRange, "revision compacted")
	case wire.OutcomeErrFutureRevision:
		return status.Error(codes.OutOfRange, "revision in the future")
	default:
		return status.Errorf(codes.Internal, "unexpected outcome %s", outcomeName(kind))
	}
}
