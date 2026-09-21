package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Answer is what one offered operation came back as.
type Answer struct {
	// Outcome is "established", "unknown", or "refused: <bounded
	// reason>". It is never a free-text error: a refusal a report
	// cannot group is a refusal a reader cannot count.
	Outcome string
	// Revision the domain acknowledged, when it acknowledged one.
	Revision int64
	// Wrote says the operation changed the key, so an event is owed for
	// it and the watcher's delay is measured against this answer.
	Wrote bool
}

// Established is the outcome name of a completed operation.
const Established = "established"

// Unknown is the outcome name of an operation whose result this caller
// never learned.
const Unknown = "unknown"

// Refused names a bounded refusal.
func Refused(reason string) string { return "refused: " + reason }

// Caller is one client of one arm: its own connection, its own session.
type Caller interface {
	// Do performs one operation.
	Do(ctx context.Context, op Operation) Answer
	// Close ends it.
	Close()
}

// Arm is one composition of the domain under measurement.
type Arm interface {
	// Name is "edge" or "backend".
	Name() string
	// Caller builds the caller at `index`.
	Caller(index int) (Caller, error)
	// Watch starts observing the workload's prefix and returns a
	// watcher, or states why this arm observes nothing.
	Watch(ctx context.Context, prefix string) (Watcher, error)
	// Stages renders what this arm could see of the breakdown, for a run
	// that offered `operations` operations.
	Stages(operations uint64) Stages
	// Close ends the arm.
	Close()
}

// Watcher records when the domain's events reached an observer.
type Watcher interface {
	// DeliveredAt is when the event for `revision` arrived, if it has.
	DeliveredAt(revision int64) (time.Time, bool)
	// Close ends the observation.
	Close()
}

// Spec is how a run is asked for.
type Spec struct {
	Label       string
	Seed        uint64
	Durability  string
	Topology    Topology
	ArrivalNs   uint64
	WarmupOps   uint64
	MeasuredOps uint64
	Callers     int
	Deadline    time.Duration
	Workload    Workload
	// ObserveEvents opens a watch beside the callers and reports the
	// delay from a write's acknowledgement to its event.
	ObserveEvents bool
}

// arrival is one scheduled unit of work.
type arrival struct {
	scheduled time.Time
	op        Operation
	measured  bool
}

// sample is one completed arrival.
type sample struct {
	kind     Kind
	measured bool
	queue    time.Duration
	service  time.Duration
	whole    time.Duration
	ackedAt  time.Time
	answer   Answer
}

// Run drives one arm and reports what happened.
func Run(ctx context.Context, arm Arm, spec Spec) (*RunV1, error) {
	if spec.Durability == "" {
		return nil, errors.New("a run has no headline without a named durability; pass -durability")
	}
	if spec.MeasuredOps == 0 {
		return nil, errors.New("a run measures at least one operation")
	}
	if spec.Callers <= 0 {
		return nil, errors.New("a run needs at least one caller")
	}

	callers := make([]Caller, 0, spec.Callers)
	defer func() {
		for _, c := range callers {
			c.Close()
		}
	}()
	for i := 0; i < spec.Callers; i++ {
		caller, err := arm.Caller(i)
		if err != nil {
			return nil, fmt.Errorf("caller %d: %w", i, err)
		}
		callers = append(callers, caller)
	}

	var watcher Watcher
	if spec.ObserveEvents {
		w, err := arm.Watch(ctx, spec.Workload.Prefix)
		if err != nil {
			return nil, fmt.Errorf("watch: %w", err)
		}
		watcher = w
		defer watcher.Close()
	}

	openLoop := spec.ArrivalNs > 0
	// A closed loop gets a channel exactly as deep as the callers, so
	// the offered rate is the achieved one by construction. An open
	// loop's is deep on purpose: the schedule is the experiment, and a
	// bound would quietly turn it back into a closed loop.
	depth := spec.Callers
	if openLoop {
		depth = int(spec.WarmupOps + spec.MeasuredOps)
	}
	arrivals := make(chan arrival, depth)
	samples := make(chan sample, depth)

	var workers sync.WaitGroup
	for _, caller := range callers {
		workers.Add(1)
		go func(c Caller) {
			defer workers.Done()
			for a := range arrivals {
				started := time.Now()
				opCtx, cancel := context.WithTimeout(ctx, spec.Deadline)
				answer := c.Do(opCtx, a.op)
				cancel()
				completed := time.Now()
				samples <- sample{
					kind:     a.op.Kind,
					measured: a.measured,
					queue:    started.Sub(a.scheduled),
					service:  completed.Sub(started),
					whole:    completed.Sub(a.scheduled),
					ackedAt:  completed,
					answer:   answer,
				}
			}
		}(caller)
	}

	rng := rand.New(rand.NewPCG(spec.Seed, spec.Seed^0x9e3779b97f4a7c15))
	total := spec.WarmupOps + spec.MeasuredOps
	origin := time.Now()
	startedAt := origin.Unix()
	measuredStart := origin
	var offered uint64

	go func() {
		for index := uint64(0); index < total; index++ {
			measured := index >= spec.WarmupOps
			scheduled := time.Now()
			if openLoop {
				scheduled = origin.Add(time.Duration(spec.ArrivalNs * index))
				if wait := time.Until(scheduled); wait > 0 {
					time.Sleep(wait)
				}
			}
			if measured && offered == 0 {
				measuredStart = scheduled
			}
			if measured {
				offered++
			}
			arrivals <- arrival{scheduled: scheduled, op: spec.Workload.Next(rng), measured: measured}
		}
		close(arrivals)
	}()

	go func() {
		workers.Wait()
		close(samples)
	}()

	type pathAcc struct {
		queue, service, whole Samples
		refusals              map[string]uint64
		samples               uint64
	}
	paths := map[Kind]*pathAcc{}
	for _, kind := range Kinds {
		paths[kind] = &pathAcc{refusals: map[string]uint64{}}
	}
	var completed, refused, unknown uint64
	writes := map[int64]time.Time{}
	measuredEnd := origin
	for s := range samples {
		if !s.measured {
			continue
		}
		acc := paths[s.kind]
		if acc == nil {
			continue
		}
		acc.samples++
		acc.queue.Add(s.queue)
		acc.service.Add(s.service)
		acc.whole.Add(s.whole)
		acc.refusals[s.answer.Outcome]++
		switch {
		case s.answer.Outcome == Established:
			completed++
		case s.answer.Outcome == Unknown:
			unknown++
		default:
			refused++
		}
		if s.ackedAt.After(measuredEnd) {
			measuredEnd = s.ackedAt
		}
		if s.answer.Wrote && s.answer.Revision > 0 {
			writes[s.answer.Revision] = s.ackedAt
		}
	}

	wall := measuredEnd.Sub(measuredStart)
	if wall <= 0 {
		wall = time.Nanosecond
	}
	perSecond := func(n uint64) *uint64 {
		v := uint64(float64(n) / wall.Seconds())
		return &v
	}

	report := &RunV1{
		Format:     FormatV1,
		Arm:        arm.Name(),
		Label:      spec.Label,
		StartedAt:  startedAt,
		Seed:       spec.Seed,
		Durability: spec.Durability,
		Topology:   spec.Topology,
		Schedule: Schedule{
			ArrivalNs:   spec.ArrivalNs,
			WarmupOps:   spec.WarmupOps,
			MeasuredOps: spec.MeasuredOps,
			Callers:     spec.Callers,
			DeadlineMs:  uint32(spec.Deadline / time.Millisecond),
			OpenLoop:    openLoop,
		},
		Achieved: Achieved{
			Offered:           offered,
			Completed:         completed,
			Refused:           refused,
			Unknown:           unknown,
			WallNs:            uint64(wall),
			OfferedPerSecond:  perSecond(offered),
			AchievedPerSecond: perSecond(completed),
		},
		Paths: map[string]Path{},
		// The trace covers the whole run, warm-up included, so the
		// count it is divided by has to be the whole run's too. A ratio
		// of measured operations to every invocation the arm performed
		// would report a create costing two commands for every warm-up
		// operation the report does not otherwise mention.
		Stages:  arm.Stages(spec.WarmupOps + offered),
		Caveats: Caveats,
	}
	for kind, acc := range paths {
		if acc.samples == 0 {
			continue
		}
		report.Paths[string(kind)] = Path{
			Samples:  acc.samples,
			Queue:    acc.queue.Percentiles(),
			Service:  acc.service.Percentiles(),
			Whole:    acc.whole.Percentiles(),
			Refusals: acc.refusals,
		}
	}
	report.Events = observedEvents(watcher, writes)
	return report, nil
}

// observedEvents joins the writes this run acknowledged with the events
// a watcher saw, and reports the delay between them. A write whose event
// never arrived is counted as missed rather than dropped: a watcher that
// is behind is a finding, not an absence of data.
func observedEvents(watcher Watcher, writes map[int64]time.Time) Events {
	if watcher == nil {
		return Events{Delivered: Missing(NotStated)}
	}
	var delay Samples
	var missed uint64
	for revision, acked := range writes {
		at, ok := watcher.DeliveredAt(revision)
		if !ok {
			missed++
			continue
		}
		delay.Add(at.Sub(acked))
	}
	return Events{
		Delivered: delay.Percentiles(),
		Observed:  uint64(delay.Len()),
		Missed:    missed,
	}
}
