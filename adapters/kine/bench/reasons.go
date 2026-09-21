package bench

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reasonOf names a failure by its bounded class, never by its text. A
// report groups refusals, and a refusal carrying a message no two runs
// spell the same way cannot be grouped.
func reasonOf(err error) string {
	if err == nil {
		return "none"
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	}
	if s, ok := status.FromError(err); ok {
		if s.Code() == codes.Unavailable && isUnknownOutcome(s.Message()) {
			// The domain may or may not have applied it. That is a
			// distinct thing from a refusal and is counted as one.
			return "unknown outcome"
		}
		return s.Code().String()
	}
	return codes.Unknown.String()
}

// isUnknownOutcome recognizes the one Unavailable the backend raises to
// say the outcome was never established, as opposed to the many it
// raises to say the domain could not be reached.
func isUnknownOutcome(message string) bool {
	const prefix = "outcome unknown after"
	return len(message) >= len(prefix) && message[:len(prefix)] == prefix
}
