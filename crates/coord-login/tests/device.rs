//! task-39 acceptance: the device grant is bounded in polling, attempts
//! and lifetime; pending, slow_down, denied and expired are distinct;
//! the user code alone grants nothing and carries no secret; approval
//! comes only through the browser leg; concurrent pollers cannot take
//! one grant twice, and replicated state consumes its commitment once.

use std::collections::BTreeMap;

use coord_login::{
    DeviceError, DeviceLimits, DeviceLogin, LoginError, Poll, Registration, UpstreamIdentity,
    normalize_user_code,
};

const IDP: &str = "https://idp.example";
const NOW: u64 = 1_700_000_000;

fn entropy(n: u8) -> [u8; 32] {
    let mut e = [0u8; 32];
    for (i, b) in e.iter_mut().enumerate() {
        *b = n.wrapping_mul(13).wrapping_add(i as u8);
    }
    e
}

fn device(limits: DeviceLimits) -> DeviceLogin {
    let mut upstream_clients = BTreeMap::new();
    upstream_clients.insert("idp".to_string(), "broker-at-idp".to_string());
    DeviceLogin::new(
        limits,
        vec![Registration {
            client_id: "coordctl".into(),
            redirect_uris: vec![],
            upstream: "idp".into(),
        }],
        upstream_clients,
        "https://auth.cluster-1/device".into(),
        "cluster-1".into(),
    )
}

fn identity() -> UpstreamIdentity {
    UpstreamIdentity {
        issuer: IDP.into(),
        subject: "user-42".into(),
        audiences: vec!["broker-at-idp".into()],
        authorized_party: None,
        expires_at: NOW + 600,
    }
}

#[test]
fn device_grants_are_bounded_single_use_and_approved_only_through_the_browser() {
    let limits = DeviceLimits {
        max_pending: 2,
        code_ttl_secs: 300,
        interval_secs: 5,
        max_polls: 6,
        max_attempts_per_window: 4,
        attempt_window_secs: 60,
    };
    let mut d = device(limits);
    assert_eq!(
        d.authorize(NOW, "other", &entropy(1)),
        Err(DeviceError::UnknownClient)
    );
    let a = d.authorize(NOW, "coordctl", &entropy(1)).unwrap();
    assert_eq!(a.user_code.len(), 9);
    assert_eq!(&a.user_code[4..5], "-");
    assert!(!a.user_code.contains('0') && !a.user_code.contains('O'));
    assert_eq!(a.device_code.len(), 64);
    assert!(
        !a.verification_uri_complete.contains(&a.device_code),
        "no secret in the URL"
    );
    assert!(
        a.verification_uri_complete
            .ends_with(&format!("user_code={}", a.user_code))
    );
    assert!(!format!("{a:?}").contains(&a.device_code), "redacted");
    assert_eq!(a.interval, 5);
    // The user code is not a device code.
    assert_eq!(
        d.poll(NOW, &a.user_code),
        Err(DeviceError::UnknownDeviceCode)
    );
    // Polling: pending, then slow_down when too fast (the interval grows).
    assert_eq!(d.poll(NOW, &a.device_code), Ok(Poll::Pending));
    assert_eq!(d.poll(NOW + 1, &a.device_code), Ok(Poll::SlowDown));
    assert_eq!(
        d.poll(NOW + 6, &a.device_code),
        Ok(Poll::SlowDown),
        "interval is now 10"
    );
    assert_eq!(
        d.poll(NOW + 20, &a.device_code),
        Ok(Poll::SlowDown),
        "interval is now 15"
    );
    assert_eq!(d.poll(NOW + 40, &a.device_code), Ok(Poll::Pending));
    // Looking up user codes is rate limited.
    let display = d.lookup(NOW, &a.user_code.to_lowercase()).unwrap();
    assert_eq!(display.client_id, "coordctl");
    assert_eq!(display.cluster, "cluster-1");
    assert_eq!(
        d.lookup(NOW, "ZZZZ-ZZZZ"),
        Err(DeviceError::UnknownUserCode)
    );
    assert_eq!(
        d.lookup(NOW, "ZZZZ-ZZZ2"),
        Err(DeviceError::UnknownUserCode)
    );
    assert_eq!(
        d.lookup(NOW, "ZZZZ-ZZZ3"),
        Err(DeviceError::UnknownUserCode)
    );
    assert_eq!(
        d.lookup(NOW, &a.user_code),
        Err(DeviceError::TooManyAttempts)
    );
    assert_eq!(
        d.lookup(NOW + 60, &a.user_code).unwrap().client_id,
        "coordctl"
    );
    // Approval only through the browser leg: no state, no approval.
    assert_eq!(
        d.complete_browser(NOW + 60, "forged", identity(), IDP),
        Err(DeviceError::UnknownTransaction)
    );
    let started = d
        .begin_browser(NOW + 60, &normalize_user_code(&a.user_code), &entropy(2))
        .unwrap();
    assert_eq!(started.upstream, "idp");
    let (verifier, upstream, nonce) = d.upstream_exchange(&started.upstream_state).unwrap();
    assert_eq!(upstream, "idp");
    assert_eq!(nonce, started.upstream_nonce);
    assert!(!verifier.is_empty());
    let mut foreign = identity();
    foreign.issuer = "https://other.example".into();
    assert_eq!(
        d.complete_browser(NOW + 61, &started.upstream_state, foreign, IDP),
        Err(DeviceError::IssuerMismatch)
    );
    let mut multi = identity();
    multi.audiences.push("other".into());
    assert_eq!(
        d.complete_browser(NOW + 61, &started.upstream_state, multi, IDP),
        Err(DeviceError::Policy(LoginError::AzpMismatch))
    );
    let commitment = d
        .complete_browser(NOW + 61, &started.upstream_state, identity(), IDP)
        .unwrap();
    assert_eq!(
        d.complete_browser(NOW + 61, &started.upstream_state, identity(), IDP),
        Err(DeviceError::UnknownTransaction),
        "the browser state is single use"
    );
    assert_eq!(
        d.deny(NOW + 62, &a.user_code),
        Err(DeviceError::AlreadyDecided)
    );
    d.publish_browser(NOW + 64, commitment).unwrap();
    assert_eq!(
        d.publish_browser(NOW + 65, commitment),
        Err(DeviceError::UnknownTransaction),
        "publication happens once"
    );
    // Concurrent pollers: the first takes the grant, the second finds it
    // spent.
    match d.poll(NOW + 70, &a.device_code) {
        Ok(Poll::Approved(r)) => {
            assert_eq!(r.commitment, commitment);
            assert_eq!(r.identity.subject, "user-42");
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(d.poll(NOW + 80, &a.device_code), Ok(Poll::Expired));
    assert_eq!(
        d.poll(NOW + 90, &a.device_code),
        Err(DeviceError::UnknownDeviceCode)
    );
    assert_eq!(d.taken, 1);
    // Denial and expiry are distinct outcomes; the pending bound and the
    // poll bound hold.
    let b = d.authorize(NOW + 100, "coordctl", &entropy(3)).unwrap();
    d.deny(NOW + 100, &b.user_code).unwrap();
    assert_eq!(d.poll(NOW + 101, &b.device_code), Ok(Poll::Denied));
    let c = d.authorize(NOW + 100, "coordctl", &entropy(4)).unwrap();
    let e = d.authorize(NOW + 100, "coordctl", &entropy(5)).unwrap();
    assert_eq!(
        d.authorize(NOW + 100, "coordctl", &entropy(6)),
        Err(DeviceError::TooManyPending)
    );
    assert_eq!(d.poll(NOW + 500, &c.device_code), Ok(Poll::Expired));
    let mut t = NOW + 100;
    let mut last = Poll::Pending;
    for _ in 0..7 {
        t += 10;
        last = d.poll(t, &e.device_code).unwrap();
    }
    assert_eq!(last, Poll::Expired, "polls are bounded");
    assert_eq!(d.pending(), 0);
    assert_eq!(d.issued, 4);
}

#[test]
fn a_verified_login_is_published_only_once_its_grant_is_committed() {
    // The browser leg used to publish approval before the grant was
    // ordered, so a commit that failed still left a device able to
    // redeem a session for a grant replicated state never accepted.
    let mut d = device(DeviceLimits::default());
    let a = d.authorize(NOW, "coordctl", &entropy(1)).unwrap();
    let started = d.begin_browser(NOW, &a.user_code, &entropy(2)).unwrap();
    let commitment = d
        .complete_browser(NOW + 1, &started.upstream_state, identity(), IDP)
        .unwrap();
    // Verified, not approved: the device keeps waiting.
    assert_eq!(d.poll(NOW + 2, &a.device_code), Ok(Poll::Pending));
    assert_eq!(d.poll(NOW + 12, &a.device_code), Ok(Poll::Pending));
    // Once committed, the next poll takes it.
    d.publish_browser(NOW + 13, commitment).unwrap();
    assert!(matches!(
        d.poll(NOW + 23, &a.device_code),
        Ok(Poll::Approved(_))
    ));
}

#[test]
fn an_upstream_refusal_answers_the_waiting_device() {
    // The callback answered the browser and recorded nothing, so the
    // device polled a grant that would never be decided until its code
    // ran out.
    let mut d = device(DeviceLimits::default());
    let a = d.authorize(NOW, "coordctl", &entropy(1)).unwrap();
    let started = d.begin_browser(NOW, &a.user_code, &entropy(2)).unwrap();
    d.deny_upstream(NOW + 1, &started.upstream_state).unwrap();
    assert_eq!(d.poll(NOW + 2, &a.device_code), Ok(Poll::Denied));
    // The upstream state is spent either way.
    assert_eq!(
        d.deny_upstream(NOW + 3, &started.upstream_state),
        Err(DeviceError::UnknownTransaction)
    );
}
