package backend

import (
	"strings"
	"testing"

	"github.com/tuplesky/tuplesky/adapters/kine/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A deterministic rejection reaches an API server as the status its
// reason describes, never as Internal and never as Unavailable. It is a
// property of the command and the state it executed against, identical
// on every replica, so nothing about it is worth retrying unchanged: a
// retryable code would have an API server loop on a refusal that can
// only ever be refused again.
func TestRejectionReasonsMapToActionableStatuses(t *testing.T) {
	want := map[wire.RejectionReason]codes.Code{
		wire.RejectedInvalid:           codes.InvalidArgument,
		wire.RejectedNamespaceMismatch: codes.InvalidArgument,
		wire.RejectedResponseTooLarge:  codes.ResourceExhausted,
		wire.RejectedTooManyEvents:     codes.ResourceExhausted,
		wire.RejectedTooManyDeletes:    codes.ResourceExhausted,
		wire.RejectedViewTooLarge:      codes.ResourceExhausted,
		wire.RejectedRetryOutOfWindow:  codes.ResourceExhausted,
		wire.RejectedCounterOverflow:   codes.FailedPrecondition,
		wire.RejectedUnsupported:       codes.Unimplemented,
		wire.RejectedRetryConflict:     codes.Aborted,
		wire.RejectedRetryTooOld:       codes.Aborted,
		wire.RejectedSessionInvalid:    codes.Unauthenticated,
		wire.RejectedRetryUnauthorized: codes.PermissionDenied,
		wire.RejectedAdmissionMismatch: codes.PermissionDenied,
	}
	for reason, code := range want {
		err := mapOutcomeError(wire.Result{Kind: wire.OutcomeErrRejected, Reason: reason})
		s, _ := status.FromError(err)
		if s.Code() != code {
			t.Fatalf("reason %s: code %s, want %s", reason, s.Code(), code)
		}
		if s.Code() == codes.Internal || s.Code() == codes.Unavailable {
			t.Fatalf("reason %s: retried or reported as a bug", reason)
		}
		// The reason is in the message: an operator reading a refused
		// apiserver write needs to know which budget or which binding
		// refused it, not only that something did.
		if !strings.Contains(s.Message(), reason.String()) {
			t.Fatalf("reason %s: message %q does not name it", reason, s.Message())
		}
	}
	if len(want) != 14 {
		t.Fatalf("the reason table has %d entries; wire names 14", len(want))
	}
	// And the outcome is named rather than reported as a number.
	if got := outcomeName(wire.OutcomeErrRejected); got != "ErrRejected" {
		t.Fatalf("outcome name %q", got)
	}
}

// A request the frontend will never admit is the caller's to change, not
// to retry: it maps to InvalidArgument, never to a retryable status.
func TestARequestTooLargeIsNotRetried(t *testing.T) {
	s, _ := status.FromError(mapWireError(codeRequestTooLarge, []byte("request too large")))
	if s.Code() != codes.InvalidArgument {
		t.Fatalf("code %s, want InvalidArgument", s.Code())
	}
	if !strings.Contains(s.Message(), "request too large") {
		t.Fatalf("message %q does not name the reason", s.Message())
	}
}

// An executed command whose result is withheld is not a refusal to admit,
// and a retired result is neither (task-d23): each has its own status and
// names what happened.
func TestAWithheldResultAndARetiredOneAreTheirOwnStatuses(t *testing.T) {
	for _, tc := range []struct {
		code uint16
		want codes.Code
		text string
	}{
		{codeOutputWithheld, codes.PermissionDenied, "executed, result withheld"},
		{codeResultRetired, codes.DataLoss, "result retired"},
	} {
		s, _ := status.FromError(mapWireError(tc.code, []byte("x")))
		if s.Code() != tc.want {
			t.Fatalf("code %#04x: status %s, want %s", tc.code, s.Code(), tc.want)
		}
		if !strings.Contains(s.Message(), tc.text) {
			t.Fatalf("code %#04x: message %q does not say %q", tc.code, s.Message(), tc.text)
		}
	}
	if s, _ := status.FromError(mapWireError(codeNotAdmitted, []byte("x"))); strings.Contains(s.Message(), "withheld") {
		t.Fatalf("not admitted reads as withheld: %q", s.Message())
	}
}
