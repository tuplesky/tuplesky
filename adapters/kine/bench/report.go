package bench

import (
	"sort"
	"time"
)

// FormatV1 identifies this report shape. It is frozen with the other
// formats of the registry.
const FormatV1 = 1

// Absent says why a number is not here. It is never "zero": a reader has
// to be able to tell "nothing happened" from "nobody measured", and the
// difference decides whether a comparison is allowed at all.
type Absent string

// The reasons.
const (
	// NoSamples: nothing was sampled.
	NoSamples Absent = "no samples"
	// NotOnThisSide: it belongs to a process this arm is outside of. The
	// native exchange seen from an etcd client is the clear case: it
	// happens inside `kine-coord`, and a number invented here would be
	// this arm's round trip wearing another stage's name.
	NotOnThisSide Absent = "measured on the other side of the edge"
	// NotStated: the operator did not state it and it is not guessable.
	NotStated Absent = "not stated"
)

// Percentiles is a distribution, named as `coord-store-bench` names it so
// the two programs' reports read the same way.
type Percentiles struct {
	Count  uint64  `json:"count"`
	MinNs  *uint64 `json:"min_ns"`
	P50Ns  *uint64 `json:"p50_ns"`
	P95Ns  *uint64 `json:"p95_ns"`
	P99Ns  *uint64 `json:"p99_ns"`
	P999Ns *uint64 `json:"p999_ns"`
	MaxNs  *uint64 `json:"max_ns"`
	MeanNs *uint64 `json:"mean_ns"`
}

// Distribution is a measurement or a stated absence.
type Distribution struct {
	Observed *Percentiles `json:"observed,omitempty"`
	Absent   Absent       `json:"absent,omitempty"`
}

// Observed wraps a measured distribution.
func Observed(p Percentiles) Distribution { return Distribution{Observed: &p} }

// Missing states why there is nothing.
func Missing(why Absent) Distribution { return Distribution{Absent: why} }

// Samples accumulates durations and renders them.
type Samples struct {
	values []uint64
	total  uint64
}

// Add records one sample.
func (s *Samples) Add(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.values = append(s.values, uint64(d))
	s.total += uint64(d)
}

// Len is how many samples there are.
func (s *Samples) Len() int { return len(s.values) }

// Percentiles renders the distribution, or states that nothing was
// sampled.
func (s *Samples) Percentiles() Distribution {
	if len(s.values) == 0 {
		return Missing(NoSamples)
	}
	sorted := append([]uint64(nil), s.values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	at := func(q float64) *uint64 {
		i := int(q * float64(len(sorted)-1))
		v := sorted[i]
		return &v
	}
	mean := s.total / uint64(len(sorted))
	return Observed(Percentiles{
		Count:  uint64(len(sorted)),
		MinNs:  &sorted[0],
		P50Ns:  at(0.50),
		P95Ns:  at(0.95),
		P99Ns:  at(0.99),
		P999Ns: at(0.999),
		MaxNs:  &sorted[len(sorted)-1],
		MeanNs: &mean,
	})
}

// Schedule is how the load was asked for, not what happened.
type Schedule struct {
	ArrivalNs   uint64 `json:"arrival_ns"`
	WarmupOps   uint64 `json:"warmup_ops"`
	MeasuredOps uint64 `json:"measured_ops"`
	Callers     int    `json:"callers"`
	DeadlineMs  uint32 `json:"deadline_ms"`
	OpenLoop    bool   `json:"open_loop"`
}

// Achieved is what happened beside what was asked for.
type Achieved struct {
	Offered           uint64  `json:"offered"`
	Completed         uint64  `json:"completed"`
	Refused           uint64  `json:"refused"`
	Unknown           uint64  `json:"unknown"`
	WallNs            uint64  `json:"wall_ns"`
	OfferedPerSecond  *uint64 `json:"offered_per_second"`
	AchievedPerSecond *uint64 `json:"achieved_per_second"`
}

// Path is one operation kind's three distributions and its refusals.
type Path struct {
	Samples uint64 `json:"samples"`
	// Queue is scheduled arrival to pickup: the coordinated-omission
	// term. Under a schedule the callers cannot keep up with, it grows
	// and says so.
	Queue Distribution `json:"queue"`
	// Service is sent to answer.
	Service Distribution `json:"service"`
	// Whole is scheduled arrival to answer, and the only one a headline
	// may quote.
	Whole Distribution `json:"whole"`
	// Refusals by bounded reason.
	Refusals map[string]uint64 `json:"refusals"`
}

// Credentials is what the workload credential cost over the whole run.
// The number that matters is not the latency: it is that the count does
// not grow with the operations, which is what "no per-operation
// federation" means in a measurement rather than in prose.
type Credentials struct {
	// Exchanges against the STS over the run.
	Exchanges int `json:"exchanges"`
	// Operations the run offered, warm-up included, so the ratio is
	// readable without cross-referencing.
	Operations uint64 `json:"operations"`
	// Absent when the arm cannot see the provider, which the edge arm
	// cannot: the provider lives in the edge process.
	Absent Absent `json:"absent,omitempty"`
}

// Commands is the native-invocation trace: how many commands one storage
// operation cost. A create that had to read first, or a write that
// federated, shows up here as a ratio above one.
type Commands struct {
	// Invocations the backend performed.
	Invocations uint64 `json:"invocations"`
	// Operations the run offered them for, warm-up included: the trace
	// covers the whole run, so the count it is read against must too.
	Operations uint64 `json:"operations"`
	// Resolutions performed after an unknown outcome. These are retries
	// of an identity, not extra logical operations, and are counted
	// apart so the ratio above stays honest.
	Resolutions uint64 `json:"resolutions"`
	// ByKind counts the logical operations the backend actually sent.
	ByKind map[string]uint64 `json:"by_kind"`
	// Absent when the arm cannot see the backend, which the edge arm
	// cannot.
	Absent Absent `json:"absent,omitempty"`
}

// Stages separates the cost the way design Section 22.3 asks for: what
// the Go codec cost, what the native exchange cost, and what the
// credential cost. A stage an arm cannot see is absent with the reason.
type Stages struct {
	// Codec is the Go postcard encode of the request plus the decode of
	// the result, per operation. It is a component of the whole and
	// never a headline of its own.
	Codec Distribution `json:"codec"`
	// Native is the frame-out-to-answer-in exchange with the frontend.
	Native Distribution `json:"native"`
	// Credentials is the workload credential's whole-run cost.
	Credentials Credentials `json:"credentials"`
	// Commands is the native-invocation trace.
	Commands Commands `json:"commands"`
}

// Events is what a watcher saw, measured from the moment the write was
// acknowledged to the caller. This is deliberately not folded into the
// write's own latency: a control plane that is waiting on its informer
// is not waiting on its write, and the two are different problems.
type Events struct {
	// Delivered is acknowledgement to event delivery.
	Delivered Distribution `json:"delivered"`
	// Observed is how many writes the watcher saw.
	Observed uint64 `json:"observed"`
	// Missed is how many it did not see before the run ended.
	Missed uint64 `json:"missed"`
	// Ahead is how many of the observed events had already been
	// delivered when the caller learned the write applied.
	//
	// A duration cannot be negative, so those samples enter the
	// distribution as zero. Counting them separately is what keeps the
	// resulting `p50 = 0` from reading as "delivered instantly": it
	// means the watcher was not waiting on the write at all, which on
	// one host is the ordinary case and on a real topology would not
	// be.
	Ahead uint64 `json:"ahead"`
}

// Topology is what the domain was, as declared. This program drives a
// domain; it does not discover one, and an inferred figure would be a
// guess in a report that must not carry guesses.
type Topology struct {
	Label      string `json:"label"`
	Voters     int    `json:"voters"`
	Endpoint   string `json:"endpoint"`
	Impairment string `json:"impairment,omitempty"`
}

// RunV1 is one measured arm.
type RunV1 struct {
	Format     int             `json:"format"`
	Arm        string          `json:"arm"`
	Label      string          `json:"label"`
	StartedAt  int64           `json:"started_at"`
	Seed       uint64          `json:"seed"`
	Durability string          `json:"durability"`
	Topology   Topology        `json:"topology"`
	Schedule   Schedule        `json:"schedule"`
	Achieved   Achieved        `json:"achieved"`
	Paths      map[string]Path `json:"paths"`
	Stages     Stages          `json:"stages"`
	Events     Events          `json:"events"`
	Caveats    []string        `json:"caveats"`
}

// Caveats every run carries, whatever it measured.
var Caveats = []string{
	"Client-observed latency only. The daemon's own stage, synchronization and " +
		"commit-return metrics are rendered on its startup and shutdown report and are " +
		"stated as absent here rather than estimated.",
	"The offered load is what the schedule asked for. Where the queue distribution " +
		"grows, the callers did not keep up and the achieved rate, not the scheduled " +
		"one, is what was measured.",
	"A single-host run measures codec, transport and consensus over loopback. It is " +
		"not a WAN result and must not be labelled as one.",
	"The credential this run presents is minted by the qualification harness rather " +
		"than federated from an identity provider. What is measured is that the count " +
		"of exchanges does not grow with the operations, not what a real exchange costs.",
	"The edge arm and the backend arm are separate compositions of the same domain. " +
		"Their difference is the edge's cost only when both were offered the same work " +
		"against the same domain in the same run; a number taken from two runs is not " +
		"that difference.",
}
