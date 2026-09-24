package wire

// The DTO field bounds owned by the Rust schema.
const (
	maxCapabilities   = 64
	maxReasonBytes    = 256
	maxKeyBytes       = 8 * 1024
	maxValueBytes     = 1024 * 1024
	maxRequestBytes   = 2*1024*1024 + 64*1024
	maxResultBytes    = 8 * 1024 * 1024
	maxEventsPerBatch = 4096
	idBytes           = 16
	digestBytes       = 32
)

// PeerRole discriminants (postcard enum order).
type PeerRole uint8

// The roles, in schema order.
const (
	RoleClient PeerRole = iota
	RoleFrontend
	RoleKineCollector
	RoleVoter
	RoleObserver
	RoleLearner
)

// RetryKey is the stable invocation identity.
type RetryKey struct {
	ClusterID       [idBytes]byte
	DomainID        [idBytes]byte
	SessionID       [idBytes]byte
	ClientInstance  [idBytes]byte
	RequestSequence uint64
}

// Message is one decoded wire_v1 message.
type Message interface {
	kind() uint16
	// validate applies the same schema bounds and discriminant checks the
	// decoder does, so a DTO built directly by a caller is refused here
	// rather than by the peer after it has crossed the wire.
	validate() error
	encode(w *writer)
}

// checkBytes refuses a byte field longer than its schema bound.
func checkBytes(b []byte, limit int) error {
	if len(b) > limit {
		return ErrMalformedPayload
	}
	return nil
}

// checkCapabilities refuses a capability list longer than its bound.
func checkCapabilities(caps []uint16) error {
	if len(caps) > maxCapabilities {
		return ErrMalformedPayload
	}
	return nil
}

// Hello is the first frame on a connection.
type Hello struct {
	Role         PeerRole
	ClusterID    [idBytes]byte
	DomainID     [idBytes]byte
	Incarnation  *uint64
	Capabilities []uint16
}

func (Hello) kind() uint16 { return uint16(KindHello) }
func (m Hello) validate() error {
	if m.Role > RoleLearner {
		return ErrMalformedPayload
	}
	return checkCapabilities(m.Capabilities)
}
func (m Hello) encode(w *writer) {
	w.varint(uint64(m.Role))
	w.raw(m.ClusterID[:])
	w.raw(m.DomainID[:])
	if m.Incarnation == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.varint(*m.Incarnation)
	}
	w.varint(uint64(len(m.Capabilities)))
	for _, c := range m.Capabilities {
		w.varint(uint64(c))
	}
}

// HelloAck acknowledges a Hello.
type HelloAck struct {
	Capabilities []uint16
	MaxInflight  uint32
}

func (HelloAck) kind() uint16      { return uint16(KindHelloAck) }
func (m HelloAck) validate() error { return checkCapabilities(m.Capabilities) }
func (m HelloAck) encode(w *writer) {
	w.varint(uint64(len(m.Capabilities)))
	for _, c := range m.Capabilities {
		w.varint(uint64(c))
	}
	w.varint(uint64(m.MaxInflight))
}

// Close is an orderly close with a reason.
type Close struct {
	Code   uint16
	Reason []byte
}

func (Close) kind() uint16      { return uint16(KindClose) }
func (m Close) validate() error { return checkBytes(m.Reason, maxReasonBytes) }
func (m Close) encode(w *writer) {
	w.varint(uint64(m.Code))
	w.boundedBytes(m.Reason)
}

func encodeRetryKey(w *writer, k RetryKey) {
	w.raw(k.ClusterID[:])
	w.raw(k.DomainID[:])
	w.raw(k.SessionID[:])
	w.raw(k.ClientInstance[:])
	w.varint(k.RequestSequence)
}

// Request carries a canonical logical request.
type Request struct {
	RetryKey   RetryKey
	Logical    []byte
	DeadlineMs uint32
}

func (Request) kind() uint16      { return uint16(KindRequest) }
func (m Request) validate() error { return checkBytes(m.Logical, maxRequestBytes) }
func (m Request) encode(w *writer) {
	encodeRetryKey(w, m.RetryKey)
	w.boundedBytes(m.Logical)
	w.varint(uint64(m.DeadlineMs))
}

// Outcome discriminants.
type OutcomeTag uint8

// The outcome tags, in schema order.
const (
	OutcomeOk OutcomeTag = iota
	OutcomeErr
	OutcomePending
	OutcomeUnknown
)

// Response is the final result of a request.
type Response struct {
	CommandID [digestBytes]byte
	Tag       OutcomeTag
	// Ok:
	HasRevision bool
	Revision    uint64
	Result      []byte
	// Err:
	Code   uint16
	Detail []byte
}

func (Response) kind() uint16 { return uint16(KindResponse) }
func (m Response) validate() error {
	switch m.Tag {
	case OutcomeOk:
		return checkBytes(m.Result, maxResultBytes)
	case OutcomeErr:
		return checkBytes(m.Detail, maxReasonBytes)
	case OutcomePending, OutcomeUnknown:
		return nil
	default:
		// An unknown tag would otherwise be written with no body, which
		// the peer reads as a malformed payload.
		return ErrMalformedPayload
	}
}
func (m Response) encode(w *writer) {
	w.raw(m.CommandID[:])
	w.varint(uint64(m.Tag))
	switch m.Tag {
	case OutcomeOk:
		if m.HasRevision {
			w.byte(1)
			w.varint(m.Revision)
		} else {
			w.byte(0)
		}
		w.boundedBytes(m.Result)
	case OutcomeErr:
		w.varint(uint64(m.Code))
		w.boundedBytes(m.Detail)
	}
}

// ResolveRequest resolves a previously submitted request.
type ResolveRequest struct {
	RetryKey  RetryKey
	CommandID [digestBytes]byte
}

func (ResolveRequest) kind() uint16    { return uint16(KindResolveRequest) }
func (ResolveRequest) validate() error { return nil }
func (m ResolveRequest) encode(w *writer) {
	encodeRetryKey(w, m.RetryKey)
	w.raw(m.CommandID[:])
}

// WatchOpen opens a watch.
type WatchOpen struct {
	WatchID        uint64
	Namespace      [idBytes]byte
	Key            []byte
	RangeEnd       *[]byte
	StartRevision  *uint64
	PrevKV         bool
	ProgressNotify bool
}

func (WatchOpen) kind() uint16 { return uint16(KindWatchOpen) }
func (m WatchOpen) validate() error {
	if err := checkBytes(m.Key, maxKeyBytes); err != nil {
		return err
	}
	if m.RangeEnd != nil {
		return checkBytes(*m.RangeEnd, maxKeyBytes)
	}
	return nil
}
func (m WatchOpen) encode(w *writer) {
	w.varint(m.WatchID)
	w.raw(m.Namespace[:])
	w.boundedBytes(m.Key)
	if m.RangeEnd == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.boundedBytes(*m.RangeEnd)
	}
	if m.StartRevision == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.varint(*m.StartRevision)
	}
	w.boolean(m.PrevKV)
	w.boolean(m.ProgressNotify)
}

// EventKind discriminants.
type EventKind uint8

// The event kinds, in schema order.
const (
	EventPut EventKind = iota
	EventDelete
)

// Event is one KV event in a watch batch.
type Event struct {
	Kind           EventKind
	Key            []byte
	Value          []byte
	CreateRevision uint64
	ModRevision    uint64
	Version        uint64
	PrevValue      *[]byte
}

// WatchEvents is one complete-revision batch.
type WatchEvents struct {
	WatchID  uint64
	Revision uint64
	Events   []Event
	Complete bool
}

func (WatchEvents) kind() uint16 { return uint16(KindWatchEvents) }
func (m WatchEvents) validate() error {
	if len(m.Events) > maxEventsPerBatch {
		return ErrMalformedPayload
	}
	for _, e := range m.Events {
		if e.Kind > EventDelete {
			return ErrMalformedPayload
		}
		if err := checkBytes(e.Key, maxKeyBytes); err != nil {
			return err
		}
		if err := checkBytes(e.Value, maxValueBytes); err != nil {
			return err
		}
		if e.PrevValue != nil {
			if err := checkBytes(*e.PrevValue, maxValueBytes); err != nil {
				return err
			}
		}
	}
	return nil
}
func (m WatchEvents) encode(w *writer) {
	w.varint(m.WatchID)
	w.varint(m.Revision)
	w.varint(uint64(len(m.Events)))
	for _, e := range m.Events {
		w.varint(uint64(e.Kind))
		w.boundedBytes(e.Key)
		w.boundedBytes(e.Value)
		w.varint(e.CreateRevision)
		w.varint(e.ModRevision)
		w.varint(e.Version)
		if e.PrevValue == nil {
			w.byte(0)
		} else {
			w.byte(1)
			w.boundedBytes(*e.PrevValue)
		}
	}
	w.boolean(m.Complete)
}

// WatchProgress notes progress through a revision.
type WatchProgress struct {
	WatchID  uint64
	Revision uint64
}

func (WatchProgress) kind() uint16    { return uint16(KindWatchProgress) }
func (WatchProgress) validate() error { return nil }
func (m WatchProgress) encode(w *writer) {
	w.varint(m.WatchID)
	w.varint(m.Revision)
}

// WatchCloseReason discriminants.
type WatchCloseReason uint8

// The close reasons, in schema order.
const (
	WatchCancelled WatchCloseReason = iota
	WatchCompacted
	WatchSlowConsumer
	WatchUnauthorized
	WatchSourceLost
)

// WatchClose closes a watch.
type WatchClose struct {
	WatchID              uint64
	Reason               WatchCloseReason
	LastCompleteRevision *uint64
}

func (WatchClose) kind() uint16 { return uint16(KindWatchClose) }
func (m WatchClose) validate() error {
	if m.Reason > WatchSourceLost {
		return ErrMalformedPayload
	}
	return nil
}
func (m WatchClose) encode(w *writer) {
	w.varint(m.WatchID)
	w.varint(uint64(m.Reason))
	if m.LastCompleteRevision == nil {
		w.byte(0)
	} else {
		w.byte(1)
		w.varint(*m.LastCompleteRevision)
	}
}

// ClusterID16to32 widens the retry key's cluster id to a 32-byte command
// id shape, used only by the in-process test server to echo an id.
func (k RetryKey) ClusterID16to32() [32]byte {
	var out [32]byte
	copy(out[:16], k.ClusterID[:])
	copy(out[16:], k.DomainID[:])
	return out
}
