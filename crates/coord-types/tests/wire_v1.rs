//! Wire codec acceptance: header truncations, length and integer overflow,
//! trailing bytes, unknown kinds/versions, oversized nested collections and
//! canonical identity payloads. Also freezes valid/invalid frame vectors.

use std::path::PathBuf;

use coord_types::ids::*;
use coord_types::logical_v1::*;
use coord_types::wire_v1::*;
use coord_types::{CommandId, RetryKey};
use serde::{Deserialize, Serialize};

fn retry_key() -> RetryKey {
    RetryKey {
        cluster_id: ClusterId(*b"cluster-00000001"),
        domain_id: DomainId(*b"domain-000000001"),
        session_id: SessionId(*b"session-00000001"),
        client_instance_id: ClientInstanceId(*b"client-000000001"),
        request_sequence: RequestSequence::new(1).unwrap(),
    }
}

fn logical() -> LogicalRequest {
    LogicalRequest::new(
        NamespaceId(*b"tenant-000000001"),
        CanonicalOperation::Put(PutOp {
            key: b"/k".to_vec(),
            value: b"v".to_vec(),
            lease: None,
            prev_kv: false,
        }),
    )
}

fn rev(n: u64) -> KvRevision {
    KvRevision::new(n).unwrap()
}

fn bytes<const N: usize>(b: &[u8]) -> BoundedBytes<N> {
    BoundedBytes::new(b.to_vec()).unwrap()
}

fn sample_messages() -> Vec<(&'static str, MessageV1)> {
    let request = RequestV1::new(retry_key(), &logical(), 5000, 0).unwrap();
    let command_id = CommandId::derive(&retry_key(), &logical()).unwrap();
    vec![
        (
            "hello-voter",
            MessageV1::Hello(HelloV1 {
                role: PeerRole::Voter,
                cluster_id: ClusterId(*b"cluster-00000001"),
                domain_id: DomainId(*b"domain-000000001"),
                incarnation: Some(ReplicaIncarnation::new(3).unwrap()),
                capabilities: BoundedVec::new(vec![1, 2, 7]).unwrap(),
            }),
        ),
        (
            "hello-client",
            MessageV1::Hello(HelloV1 {
                role: PeerRole::Client,
                cluster_id: ClusterId(*b"cluster-00000001"),
                domain_id: DomainId(*b"domain-000000001"),
                incarnation: None,
                capabilities: BoundedVec::new(vec![]).unwrap(),
            }),
        ),
        (
            "hello-ack",
            MessageV1::HelloAck(HelloAckV1 {
                capabilities: BoundedVec::new(vec![1]).unwrap(),
                max_inflight: 64,
            }),
        ),
        (
            "close",
            MessageV1::Close(CloseV1 {
                code: 3,
                reason: bytes(b"draining"),
            }),
        ),
        ("request-put", MessageV1::Request(request)),
        (
            "response-ok",
            MessageV1::Response(ResponseV1 {
                command_id,
                outcome: OutcomeV1::Ok {
                    revision: Some(rev(12)),
                    result: bytes(&[0, 1, 2]),
                },
            }),
        ),
        (
            "response-err",
            MessageV1::Response(ResponseV1 {
                command_id,
                outcome: OutcomeV1::Err {
                    code: 9,
                    detail: bytes(b"denied"),
                },
            }),
        ),
        (
            "response-pending",
            MessageV1::Response(ResponseV1 {
                command_id,
                outcome: OutcomeV1::Pending,
            }),
        ),
        (
            "response-unknown",
            MessageV1::Response(ResponseV1 {
                command_id,
                outcome: OutcomeV1::Unknown,
            }),
        ),
        (
            "resolve",
            MessageV1::ResolveRequest(ResolveRequestV1 {
                retry_key: retry_key(),
                command_id,
            }),
        ),
        (
            "watch-open",
            MessageV1::WatchOpen(WatchOpenV1 {
                watch_id: 1,
                namespace: NamespaceId(*b"tenant-000000001"),
                key: bytes(b"/a"),
                range_end: Some(bytes(b"/b")),
                start_revision: Some(rev(100)),
                prev_kv: true,
                progress_notify: true,
            }),
        ),
        (
            "watch-events",
            MessageV1::WatchEvents(WatchEventsV1 {
                watch_id: 1,
                revision: rev(101),
                events: BoundedVec::new(vec![
                    EventV1 {
                        kind: EventKindV1::Put,
                        key: bytes(b"/a/1"),
                        value: bytes(b"x"),
                        create_revision: rev(101),
                        mod_revision: rev(101),
                        version: 1,
                        prev_value: None,
                    },
                    EventV1 {
                        kind: EventKindV1::Delete,
                        key: bytes(b"/a/2"),
                        value: bytes(b""),
                        create_revision: rev(50),
                        mod_revision: rev(101),
                        version: 0,
                        prev_value: Some(bytes(b"old")),
                    },
                ])
                .unwrap(),
                complete: true,
            }),
        ),
        (
            "watch-progress",
            MessageV1::WatchProgress(WatchProgressV1 {
                watch_id: 1,
                revision: rev(150),
            }),
        ),
        (
            "watch-close",
            MessageV1::WatchClose(WatchCloseV1 {
                watch_id: 1,
                reason: WatchCloseReasonV1::Compacted,
                last_complete_revision: Some(rev(150)),
            }),
        ),
    ]
}

#[test]
fn every_message_round_trips_and_streams_concatenate() {
    let mut stream = Vec::new();
    let messages = sample_messages();
    for (_, m) in &messages {
        let frame = m.encode().unwrap();
        assert_eq!(decode_stream(&frame).unwrap(), vec![m.clone()]);
        stream.extend_from_slice(&frame);
    }
    let decoded = decode_stream(&stream).unwrap();
    assert_eq!(decoded.len(), messages.len());
    for (i, (_, m)) in messages.iter().enumerate() {
        assert_eq!(&decoded[i], m);
    }
    // Feeding the stream one byte at a time yields the same frames.
    let mut reader = FrameReader::new();
    let mut count = 0;
    for b in &stream {
        reader.push(&[*b]).unwrap();
        while let Some(frame) = reader.next_frame().unwrap() {
            decode(&frame).unwrap();
            count += 1;
        }
    }
    reader.finish().unwrap();
    assert_eq!(count, messages.len());
}

#[test]
fn every_header_truncation_is_incomplete_never_a_frame() {
    let frame = sample_messages()[3].1.encode().unwrap();
    for cut in 1..frame.len() {
        let partial = &frame[..cut];
        let mut reader = FrameReader::new();
        reader.push(partial).unwrap();
        assert_eq!(reader.next_frame().unwrap(), None, "cut at {cut}");
        let err = reader.finish().unwrap_err();
        assert!(
            matches!(err, WireError::IncompleteFrame { .. }),
            "cut at {cut}: {err:?}"
        );
        assert!(matches!(
            decode_stream(partial),
            Err(WireError::IncompleteFrame { .. })
        ));
    }
    assert!(decode_stream(&[]).unwrap().is_empty());
}

#[test]
fn length_below_minimum_and_above_class_limit_fail_before_allocation() {
    for length in 0..4u32 {
        let mut header = [0u8; 8];
        header[..4].copy_from_slice(&length.to_be_bytes());
        header[4..6].copy_from_slice(&MessageKind::Close.as_u16().to_be_bytes());
        assert_eq!(
            check_header(&header),
            Err(WireError::LengthBelowMinimum { length })
        );
    }
    let mut header = [0u8; 8];
    let huge = u32::MAX;
    header[..4].copy_from_slice(&huge.to_be_bytes());
    header[4..6].copy_from_slice(&MessageKind::Close.as_u16().to_be_bytes());
    assert_eq!(
        check_header(&header),
        Err(WireError::LengthAboveClassLimit {
            length: huge,
            limit: KindRange::Negotiation.max_frame_length()
        })
    );
    // One byte above the limit of the negotiation class, without any payload
    // present: rejected from the header alone.
    let over = KindRange::Negotiation.max_frame_length() + 1;
    header[..4].copy_from_slice(&over.to_be_bytes());
    let mut reader = FrameReader::new();
    reader.push(&header).unwrap();
    assert!(matches!(
        reader.next_frame(),
        Err(WireError::LengthAboveClassLimit { .. })
    ));
    // Once the invalid header is buffered, no further byte is accepted.
    assert!(matches!(
        reader.push(&[0]),
        Err(WireError::LengthAboveClassLimit { .. })
    ));
    // Unregistered kinds get the smallest limit.
    header[4..6].copy_from_slice(&0xffffu16.to_be_bytes());
    header[..4].copy_from_slice(&(KindRange::ReadFence.max_frame_length() + 1).to_be_bytes());
    assert!(matches!(
        check_header(&header),
        Err(WireError::LengthAboveClassLimit { .. })
    ));
    // The encoder refuses oversized payloads for the same reason.
    let big = vec![0u8; KindRange::Negotiation.max_frame_length() as usize];
    assert_eq!(
        encode_frame(MessageKind::Close.as_u16(), 1, &big),
        Err(WireError::PayloadTooLarge)
    );
}

#[test]
fn trailing_bytes_after_a_frame_or_inside_a_payload_are_rejected() {
    let frame = sample_messages()[3].1.encode().unwrap();
    let mut with_trailing = frame.clone();
    with_trailing.extend_from_slice(&[0, 0, 0]);
    assert!(matches!(
        decode_stream(&with_trailing),
        Err(WireError::IncompleteFrame { have: 3, need: 8 })
    ));
    // Extra byte inside the payload (length covers it).
    let mut inner = frame.clone();
    inner.push(0);
    let len = u32::from_be_bytes([inner[0], inner[1], inner[2], inner[3]]) + 1;
    inner[..4].copy_from_slice(&len.to_be_bytes());
    assert_eq!(
        decode_stream(&inner),
        Err(WireError::TrailingPayloadBytes { extra: 1 })
    );
}

#[test]
fn unknown_kinds_and_versions_fail_closed() {
    let frame = sample_messages()[3].1.encode().unwrap();
    let mut unknown_kind = frame.clone();
    unknown_kind[4..6].copy_from_slice(&0x00ffu16.to_be_bytes());
    assert_eq!(
        decode_stream(&unknown_kind),
        Err(WireError::UnsupportedKind { kind: 0x00ff })
    );
    let mut reserved_range = frame.clone();
    reserved_range[4..6].copy_from_slice(&0x0300u16.to_be_bytes());
    assert_eq!(
        decode_stream(&reserved_range),
        Err(WireError::UnsupportedKind { kind: 0x0300 })
    );
    let mut bad_version = frame.clone();
    bad_version[6..8].copy_from_slice(&2u16.to_be_bytes());
    assert_eq!(
        decode_stream(&bad_version),
        Err(WireError::UnsupportedVersion {
            kind: MessageKind::Close,
            version: 2
        })
    );
    let mut zero_version = frame;
    zero_version[6..8].copy_from_slice(&0u16.to_be_bytes());
    assert!(matches!(
        decode_stream(&zero_version),
        Err(WireError::UnsupportedVersion { .. })
    ));
    for kind in [
        0x0000u16, 0x0004, 0x0103, 0x0204, 0x0400, 0x0500, 0x0600, 0x0700, 0x0800, 0x0900, 0xffff,
    ] {
        assert_eq!(
            MessageKind::from_u16(kind),
            None,
            "{kind:#06x} must not be registered"
        );
    }
    for kind in [
        MessageKind::Hello,
        MessageKind::Request,
        MessageKind::WatchEvents,
    ] {
        assert_eq!(MessageKind::from_u16(kind.as_u16()), Some(kind));
    }
    assert_eq!(KindRange::of(0x0900), None);
}

#[test]
fn integer_overflow_in_payload_is_malformed() {
    // HelloAckV1 { capabilities: [], max_inflight: u32 }. A varint that
    // overflows u32 must be rejected rather than truncated.
    let mut payload = vec![0u8]; // empty capabilities
    payload.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0x0f]); // > u32::MAX
    let frame = encode_frame(MessageKind::HelloAck.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
    // A varint with more continuation bytes than any integer allows.
    let payload = vec![
        0u8, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01,
    ];
    let frame = encode_frame(MessageKind::HelloAck.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
    // Bad enum discriminant for PeerRole.
    let mut payload = vec![9u8];
    payload.extend_from_slice(&[0u8; 32]);
    payload.push(0);
    payload.push(0);
    let frame = encode_frame(MessageKind::Hello.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
}

#[test]
fn oversized_nested_collections_are_rejected_before_allocation() {
    // capabilities declared length = MAX_CAPABILITIES + 1 with no elements present.
    let declared = (MAX_CAPABILITIES + 1) as u8;
    let payload = vec![declared];
    let frame = encode_frame(MessageKind::HelloAck.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
    // A declared byte-string length far beyond the frame: postcard checks
    // availability first, so this is malformed, not an allocation.
    let mut payload = vec![3u8]; // Close.code = 3 (varint)
    payload.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x07]); // reason length ~ 2^31
    let frame = encode_frame(MessageKind::Close.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
    // Present but over the bound: reason of MAX_REASON_BYTES + 1 bytes.
    let mut payload = vec![3u8];
    let n = MAX_REASON_BYTES + 1;
    payload.push((n & 0x7f) as u8 | 0x80);
    payload.push((n >> 7) as u8);
    payload.extend(std::iter::repeat_n(b'x', n));
    let frame = encode_frame(MessageKind::Close.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
    // Constructors enforce the same bounds.
    assert!(BoundedBytes::<4>::new(vec![0; 5]).is_err());
    assert!(BoundedVec::<u8, 2>::new(vec![0; 3]).is_err());
    // Too many watch events: declared MAX_EVENTS_PER_BATCH + 1 rejected from the length prefix.
    let mut payload = vec![1u8]; // watch_id
    payload.push(101); // revision
    let n = MAX_EVENTS_PER_BATCH + 1;
    payload.push((n & 0x7f) as u8 | 0x80);
    payload.push((n >> 7) as u8);
    let frame = encode_frame(MessageKind::WatchEvents.as_u16(), 1, &payload).unwrap();
    assert_eq!(decode_stream(&frame), Err(WireError::MalformedPayload));
}

#[test]
fn identity_payloads_must_be_canonical() {
    let request = RequestV1::new(retry_key(), &logical(), 0, 0).unwrap();
    assert_eq!(request.logical().unwrap(), logical());
    // Same identity from the wire form as from the logical form.
    let from_wire = CommandId::derive(&request.retry_key, &request.logical().unwrap()).unwrap();
    assert_eq!(
        from_wire,
        CommandId::derive(&retry_key(), &logical()).unwrap()
    );

    // Non-canonical transaction bytes (unsorted compares) are rejected even
    // though they decode.
    let c1 = Compare {
        key: b"b".to_vec(),
        target: CompareTarget::Version,
        result: CompareResult::Equal,
        operand: CompareOperand::Counter(1),
    };
    let c2 = Compare {
        key: b"a".to_vec(),
        target: CompareTarget::Version,
        result: CompareResult::Equal,
        operand: CompareOperand::Counter(1),
    };
    let unsorted = LogicalRequest::new(
        NamespaceId(*b"tenant-000000001"),
        CanonicalOperation::Txn(TxnOp {
            compares: vec![c1, c2],
            success: vec![],
            failure: vec![],
        }),
    );
    let raw = postcard::to_allocvec(&unsorted).unwrap();
    let smuggled = RequestV1 {
        ack_through: 0,
        retry_key: retry_key(),
        logical: BoundedBytes::new(raw).unwrap(),
        deadline_ms: 0,
    };
    assert_eq!(smuggled.logical(), Err(WireError::NonCanonicalPayload));
    assert!(RequestV1::new(retry_key(), &unsorted, 0, 0).is_err());

    // Trailing bytes inside the logical payload are rejected.
    let mut raw = logical().canonical_bytes().unwrap();
    raw.push(0);
    let padded = RequestV1 {
        ack_through: 0,
        retry_key: retry_key(),
        logical: BoundedBytes::new(raw).unwrap(),
        deadline_ms: 0,
    };
    assert_eq!(
        padded.logical(),
        Err(WireError::TrailingPayloadBytes { extra: 1 })
    );
    // Garbage is malformed.
    let garbage = RequestV1 {
        ack_through: 0,
        retry_key: retry_key(),
        logical: BoundedBytes::new(vec![0xff; 3]).unwrap(),
        deadline_ms: 0,
    };
    assert_eq!(garbage.logical(), Err(WireError::MalformedPayload));
}

#[test]
fn fuzz_entry_survives_mutations_of_every_valid_frame() {
    // Deterministic corpus replay: bit flips, truncations and length edits
    // of every sample frame must never panic.
    for (_, m) in sample_messages() {
        let frame = m.encode().unwrap();
        fuzz_entry(&frame);
        for i in 0..frame.len() {
            let mut mutated = frame.clone();
            mutated[i] ^= 0x01;
            fuzz_entry(&mutated);
            mutated[i] = 0xff;
            fuzz_entry(&mutated);
            mutated[i] = 0x00;
            fuzz_entry(&mutated);
            fuzz_entry(&frame[..i]);
            fuzz_entry(&frame[i..]);
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct FrameVector {
    name: String,
    frame_hex: String,
    /// `"ok"` or the error variant name.
    expect: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct FrameFixture {
    schema: String,
    header: String,
    kinds: Vec<(String, u16)>,
    vectors: Vec<FrameVector>,
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn error_name(e: &WireError) -> String {
    let s = format!("{e:?}");
    s.split([' ', '{']).next().unwrap().to_owned()
}

#[test]
fn frame_vectors_are_frozen() {
    let mut vectors = Vec::new();
    for (name, m) in sample_messages() {
        let frame = m.encode().unwrap();
        vectors.push(FrameVector {
            name: format!("valid-{name}"),
            frame_hex: hex(&frame),
            expect: "ok".to_owned(),
        });
    }
    let close = sample_messages()[3].1.encode().unwrap();
    let mut invalid: Vec<(String, Vec<u8>)> = Vec::new();
    for cut in 1..close.len() {
        invalid.push((format!("truncated-at-{cut}"), close[..cut].to_vec()));
    }
    let mut v = close.clone();
    v[..4].copy_from_slice(&3u32.to_be_bytes());
    invalid.push(("length-below-minimum".into(), v));
    let mut v = close.clone();
    v[..4].copy_from_slice(&(KindRange::Negotiation.max_frame_length() + 1).to_be_bytes());
    invalid.push(("length-above-class-limit".into(), v));
    let mut v = close.clone();
    v.push(0);
    invalid.push(("trailing-stream-byte".into(), v));
    let mut v = close.clone();
    v.push(0);
    let len = u32::from_be_bytes([v[0], v[1], v[2], v[3]]) + 1;
    v[..4].copy_from_slice(&len.to_be_bytes());
    invalid.push(("trailing-payload-byte".into(), v));
    let mut v = close.clone();
    v[4..6].copy_from_slice(&0x0300u16.to_be_bytes());
    invalid.push(("reserved-kind".into(), v));
    let mut v = close.clone();
    v[6..8].copy_from_slice(&2u16.to_be_bytes());
    invalid.push(("unsupported-version".into(), v));
    let mut payload = vec![0u8];
    payload.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0x0f]);
    invalid.push((
        "u32-varint-overflow".into(),
        encode_frame(MessageKind::HelloAck.as_u16(), 1, &payload).unwrap(),
    ));
    invalid.push((
        "collection-over-bound".into(),
        encode_frame(
            MessageKind::HelloAck.as_u16(),
            1,
            &[(MAX_CAPABILITIES + 1) as u8],
        )
        .unwrap(),
    ));
    for (name, bytes) in invalid {
        let err = decode_stream(&bytes).unwrap_err();
        vectors.push(FrameVector {
            name: format!("invalid-{name}"),
            frame_hex: hex(&bytes),
            expect: error_name(&err),
        });
    }
    let kinds = [
        MessageKind::Hello,
        MessageKind::HelloAck,
        MessageKind::Close,
        MessageKind::Request,
        MessageKind::Response,
        MessageKind::ResolveRequest,
        MessageKind::WatchOpen,
        MessageKind::WatchEvents,
        MessageKind::WatchProgress,
        MessageKind::WatchClose,
    ]
    .iter()
    .map(|k| (format!("{k:?}"), k.as_u16()))
    .collect();
    let fixture = FrameFixture {
        schema: "wire_frames_v1".to_owned(),
        header: "u32_be length (excludes itself, includes kind+version) || u16_be kind || u16_be version || postcard".to_owned(),
        kinds,
        vectors,
    };
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join("wire_frames_v1.json");
    if std::env::var_os("COORD_TYPES_WRITE_FIXTURES").is_some() {
        let mut json = serde_json::to_string_pretty(&fixture).unwrap();
        json.push('\n');
        std::fs::write(&path, json).unwrap();
        return;
    }
    let stored: FrameFixture =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(
        stored, fixture,
        "wire frame vectors drifted; this is a frozen encoding"
    );
    // Every stored vector replays to its recorded expectation.
    for v in &stored.vectors {
        let bytes: Vec<u8> = (0..v.frame_hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&v.frame_hex[i..i + 2], 16).unwrap())
            .collect();
        match decode_stream(&bytes) {
            Ok(_) => assert_eq!(v.expect, "ok", "{}", v.name),
            Err(e) => assert_eq!(v.expect, error_name(&e), "{}", v.name),
        }
    }
}

#[test]
fn reader_compacts_consumed_bytes_and_caps_pending_bytes() {
    use coord_types::wire_v1::MAX_PENDING_BYTES;
    let frame = sample_messages()[3].1.encode().unwrap();
    // Fragmented reads that always end partway into the next frame must not
    // retain consumed prefixes: the allocation stays bounded by one frame.
    let mut stream = Vec::new();
    for _ in 0..64 {
        stream.extend_from_slice(&frame);
    }
    let chunk = frame.len() / 2 + 3;
    let mut reader = FrameReader::new();
    let mut count = 0;
    for piece in stream.chunks(chunk) {
        reader.push(piece).unwrap();
        while reader.next_frame().unwrap().is_some() {
            count += 1;
        }
        assert!(
            reader.buffered() <= frame.len() + chunk,
            "buffer grew to {}",
            reader.buffered()
        );
    }
    assert_eq!(count, 64);
    reader.finish().unwrap();

    // A single push beyond the largest frame class is refused before copying.
    let mut reader = FrameReader::new();
    let too_much = vec![0u8; MAX_PENDING_BYTES + 1];
    assert!(matches!(
        reader.push(&too_much),
        Err(WireError::ReceiveBufferFull { .. })
    ));
    assert_eq!(reader.pending(), 0);
    // A valid largest-class header whose payload is one byte short fills the
    // buffer up to the bound; undrained bytes past it are refused.
    let mut partial = vec![0u8; MAX_PENDING_BYTES - 1];
    partial[..4].copy_from_slice(&KindRange::Watch.max_frame_length().to_be_bytes());
    partial[4..6].copy_from_slice(&0x0200u16.to_be_bytes());
    reader.push(&partial).unwrap();
    assert_eq!(reader.next_frame().unwrap(), None);
    assert!(matches!(
        reader.push(&[0, 0]),
        Err(WireError::ReceiveBufferFull { .. })
    ));
    assert_eq!(reader.pending(), MAX_PENDING_BYTES - 1);
}
