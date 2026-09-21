// Package bench measures what the Kubernetes storage path costs on top
// of the native one (task-63; design Sections 14.1, 14.3, 19.5, 22.3).
//
// It offers the same shaped work down two compositions of the same
// domain and reports them in the same shape, so that the difference is
// the composition rather than the workload:
//
//   - the *edge* arm is what an API server drives: an etcd v3 client
//     over mutual TLS into `kine-coord`, through the pinned Kine bridge
//     and the coord:// backend, and on to a frontend. Every mutation is
//     the guarded transaction the API server writes, because that is the
//     only mutation shape the bridge is given.
//   - the *backend* arm is the same coord:// backend with the etcd edge
//     taken away: the Go codec, the Go QUIC client and the workload
//     credential, called directly. It exists so the edge's own cost is a
//     subtraction between two measured arms instead of an estimate.
//
// The native arm is `coord-wan-bench`, which is a separate program
// because it is a separate client library; this package does not
// reimplement it and does not quote it.
//
// Three things it is careful about:
//
//   - A stage it did not measure is stated as absent with the reason,
//     never as a zero. The edge arm cannot see the native exchange from
//     outside the edge process and says so.
//   - Event observation is reported separately from write
//     acknowledgement. A watch that is behind is not a write that was
//     slow, and a page that averaged them would hide the thing a
//     control-plane measurement is for.
//   - The trace of native invocations is kept and published: one
//     storage operation costing exactly one native command is the
//     observable requirement of design Section 14.1, and it is reported
//     as a count rather than asserted in prose.
package bench
