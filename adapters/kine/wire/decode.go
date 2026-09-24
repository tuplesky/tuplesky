package wire

// Decode parses a frame into its typed message, rejecting unregistered
// kinds and unsupported versions before the payload, and trailing
// payload bytes after the DTO.
func Decode(frame Frame) (Message, error) {
	if !registered(frame.Kind) {
		if _, ok := kindClass(frame.Kind); !ok {
			return nil, decodeErr(ErrUnsupportedKind, "reserved range")
		}
		return nil, decodeErr(ErrUnsupportedKind, "unknown kind")
	}
	if frame.Version != 1 {
		return nil, decodeErr(ErrUnsupportedVersion, "only version 1")
	}
	r := newReader(frame.Payload)
	msg, err := decodePayload(Kind(frame.Kind), r)
	if err != nil {
		return nil, err
	}
	if !r.done() {
		return nil, ErrTrailingPayloadBytes
	}
	return msg, nil
}

func readID(r *reader) ([idBytes]byte, error) {
	var out [idBytes]byte
	raw, err := r.bytesN(idBytes)
	if err != nil {
		return out, err
	}
	copy(out[:], raw)
	return out, nil
}

func readDigest(r *reader) ([digestBytes]byte, error) {
	var out [digestBytes]byte
	raw, err := r.bytesN(digestBytes)
	if err != nil {
		return out, err
	}
	copy(out[:], raw)
	return out, nil
}

func readCapabilities(r *reader) ([]uint16, error) {
	n, err := r.length(maxCapabilities)
	if err != nil {
		return nil, err
	}
	caps := make([]uint16, 0, n)
	for i := 0; i < n; i++ {
		c, err := r.u16()
		if err != nil {
			return nil, err
		}
		caps = append(caps, c)
	}
	return caps, nil
}

func readRetryKey(r *reader) (RetryKey, error) {
	var k RetryKey
	var err error
	if k.ClusterID, err = readID(r); err != nil {
		return k, err
	}
	if k.DomainID, err = readID(r); err != nil {
		return k, err
	}
	if k.SessionID, err = readID(r); err != nil {
		return k, err
	}
	if k.ClientInstance, err = readID(r); err != nil {
		return k, err
	}
	if k.RequestSequence, err = r.u64(); err != nil {
		return k, err
	}
	return k, nil
}

func decodePayload(kind Kind, r *reader) (Message, error) {
	switch kind {
	case KindHello:
		return decodeHello(r)
	case KindHelloAck:
		return decodeHelloAck(r)
	case KindClose:
		return decodeClose(r)
	case KindRequest:
		return decodeRequest(r)
	case KindResponse:
		return decodeResponse(r)
	case KindResolveRequest:
		return decodeResolve(r)
	case KindWatchOpen:
		return decodeWatchOpen(r)
	case KindWatchEvents:
		return decodeWatchEvents(r)
	case KindWatchProgress:
		return decodeWatchProgress(r)
	case KindWatchClose:
		return decodeWatchClose(r)
	default:
		return nil, ErrUnsupportedKind
	}
}

func decodeHello(r *reader) (Message, error) {
	role, err := r.varint(1)
	if err != nil {
		return nil, err
	}
	if role > uint64(RoleLearner) {
		return nil, ErrMalformedPayload
	}
	cluster, err := readID(r)
	if err != nil {
		return nil, err
	}
	domain, err := readID(r)
	if err != nil {
		return nil, err
	}
	has, err := r.option()
	if err != nil {
		return nil, err
	}
	var inc *uint64
	if has {
		v, err := r.u64()
		if err != nil {
			return nil, err
		}
		inc = &v
	}
	caps, err := readCapabilities(r)
	if err != nil {
		return nil, err
	}
	return Hello{Role: PeerRole(role), ClusterID: cluster, DomainID: domain, Incarnation: inc, Capabilities: caps}, nil
}

func decodeHelloAck(r *reader) (Message, error) {
	caps, err := readCapabilities(r)
	if err != nil {
		return nil, err
	}
	inflight, err := r.u32()
	if err != nil {
		return nil, err
	}
	return HelloAck{Capabilities: caps, MaxInflight: inflight}, nil
}

func decodeClose(r *reader) (Message, error) {
	code, err := r.u16()
	if err != nil {
		return nil, err
	}
	reason, err := r.boundedBytes(maxReasonBytes)
	if err != nil {
		return nil, err
	}
	return Close{Code: code, Reason: reason}, nil
}

func decodeRequest(r *reader) (Message, error) {
	key, err := readRetryKey(r)
	if err != nil {
		return nil, err
	}
	logical, err := r.boundedBytes(maxRequestBytes)
	if err != nil {
		return nil, err
	}
	deadline, err := r.u32()
	if err != nil {
		return nil, err
	}
	return Request{RetryKey: key, Logical: logical, DeadlineMs: deadline}, nil
}

func decodeResponse(r *reader) (Message, error) {
	command, err := readDigest(r)
	if err != nil {
		return nil, err
	}
	tag, err := r.varint(1)
	if err != nil {
		return nil, err
	}
	m := Response{CommandID: command, Tag: OutcomeTag(tag)}
	switch OutcomeTag(tag) {
	case OutcomeOk:
		has, err := r.option()
		if err != nil {
			return nil, err
		}
		m.HasRevision = has
		if has {
			if m.Revision, err = r.u64(); err != nil {
				return nil, err
			}
		}
		if m.Result, err = r.boundedBytes(maxResultBytes); err != nil {
			return nil, err
		}
	case OutcomeErr:
		if m.Code, err = r.u16(); err != nil {
			return nil, err
		}
		if m.Detail, err = r.boundedBytes(maxReasonBytes); err != nil {
			return nil, err
		}
	case OutcomePending, OutcomeUnknown:
	default:
		return nil, ErrMalformedPayload
	}
	return m, nil
}

func decodeResolve(r *reader) (Message, error) {
	key, err := readRetryKey(r)
	if err != nil {
		return nil, err
	}
	command, err := readDigest(r)
	if err != nil {
		return nil, err
	}
	return ResolveRequest{RetryKey: key, CommandID: command}, nil
}

func decodeWatchOpen(r *reader) (Message, error) {
	m := WatchOpen{}
	var err error
	if m.WatchID, err = r.u64(); err != nil {
		return nil, err
	}
	if m.Namespace, err = readID(r); err != nil {
		return nil, err
	}
	if m.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
		return nil, err
	}
	has, err := r.option()
	if err != nil {
		return nil, err
	}
	if has {
		end, err := r.boundedBytes(maxKeyBytes)
		if err != nil {
			return nil, err
		}
		m.RangeEnd = &end
	}
	has, err = r.option()
	if err != nil {
		return nil, err
	}
	if has {
		v, err := r.u64()
		if err != nil {
			return nil, err
		}
		m.StartRevision = &v
	}
	if m.PrevKV, err = r.boolean(); err != nil {
		return nil, err
	}
	if m.ProgressNotify, err = r.boolean(); err != nil {
		return nil, err
	}
	return m, nil
}

func decodeWatchEvents(r *reader) (Message, error) {
	m := WatchEvents{}
	var err error
	if m.WatchID, err = r.u64(); err != nil {
		return nil, err
	}
	if m.Revision, err = r.u64(); err != nil {
		return nil, err
	}
	n, err := r.length(maxEventsPerBatch)
	if err != nil {
		return nil, err
	}
	m.Events = make([]Event, 0, n)
	for i := 0; i < n; i++ {
		var e Event
		kind, err := r.varint(1)
		if err != nil {
			return nil, err
		}
		if kind > uint64(EventDelete) {
			return nil, ErrMalformedPayload
		}
		e.Kind = EventKind(kind)
		if e.Key, err = r.boundedBytes(maxKeyBytes); err != nil {
			return nil, err
		}
		if e.Value, err = r.boundedBytes(maxValueBytes); err != nil {
			return nil, err
		}
		if e.CreateRevision, err = r.u64(); err != nil {
			return nil, err
		}
		if e.ModRevision, err = r.u64(); err != nil {
			return nil, err
		}
		if e.Version, err = r.u64(); err != nil {
			return nil, err
		}
		has, err := r.option()
		if err != nil {
			return nil, err
		}
		if has {
			pv, err := r.boundedBytes(maxValueBytes)
			if err != nil {
				return nil, err
			}
			e.PrevValue = &pv
		}
		m.Events = append(m.Events, e)
	}
	if m.Complete, err = r.boolean(); err != nil {
		return nil, err
	}
	return m, nil
}

func decodeWatchProgress(r *reader) (Message, error) {
	watch, err := r.u64()
	if err != nil {
		return nil, err
	}
	rev, err := r.u64()
	if err != nil {
		return nil, err
	}
	return WatchProgress{WatchID: watch, Revision: rev}, nil
}

func decodeWatchClose(r *reader) (Message, error) {
	watch, err := r.u64()
	if err != nil {
		return nil, err
	}
	reason, err := r.varint(1)
	if err != nil {
		return nil, err
	}
	if reason > uint64(WatchSourceLost) {
		return nil, ErrMalformedPayload
	}
	m := WatchClose{WatchID: watch, Reason: WatchCloseReason(reason)}
	has, err := r.option()
	if err != nil {
		return nil, err
	}
	if has {
		v, err := r.u64()
		if err != nil {
			return nil, err
		}
		m.LastCompleteRevision = &v
	}
	return m, nil
}

// Encode encodes a message into a complete frame (version 1). It refuses,
// with ErrMalformedPayload, any DTO whose fields Decode would refuse, so a
// value the Rust peer cannot read never leaves the process; the frame
// class limit alone is far looser than the per-field bounds.
func Encode(m Message) ([]byte, error) {
	if err := m.validate(); err != nil {
		return nil, err
	}
	var w writer
	m.encode(&w)
	return EncodeFrame(m.kind(), 1, w.buf)
}
