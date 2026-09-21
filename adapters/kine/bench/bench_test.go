package bench

import (
	"math/rand/v2"
	"testing"
	"time"
)

// The same seed offers the same work. A comparison between two arms is
// a comparison between compositions only if the workload is the same
// one, so this is the property the whole measurement rests on.
func TestOneSeedOffersOneSequence(t *testing.T) {
	workload := Workload{
		Prefix: "/registry/bench/", Keyspace: 64, HotKeys: 4,
		ValueBytes: 8, ScanLimit: 16, Mix: ControlPlane,
	}
	draw := func() []Operation {
		rng := rand.New(rand.NewPCG(7, 11))
		out := make([]Operation, 0, 64)
		for i := 0; i < 64; i++ {
			out = append(out, workload.Next(rng))
		}
		return out
	}
	first, second := draw(), draw()
	for i := range first {
		if first[i].Kind != second[i].Kind || first[i].Key != second[i].Key {
			t.Fatalf("operation %d differs: %+v vs %+v", i, first[i], second[i])
		}
		if string(first[i].Value) != string(second[i].Value) {
			t.Fatalf("operation %d carries different bytes", i)
		}
	}
}

// Every kind the mix gives weight to is offered, and no kind it does
// not. A weight silently ignored would make a row report a workload
// nobody asked for.
func TestTheMixIsWhatIsOffered(t *testing.T) {
	workload := Workload{
		Prefix: "/registry/bench/", Keyspace: 8, HotKeys: 2,
		ValueBytes: 4, ScanLimit: 8,
		Mix: Mix{Put: 1, Get: 1, CompareAndSwap: 0, Scan: 0},
	}
	rng := rand.New(rand.NewPCG(1, 2))
	seen := map[Kind]int{}
	for i := 0; i < 400; i++ {
		seen[workload.Next(rng).Kind]++
	}
	if seen[KindCompareAndSwap] != 0 || seen[KindScan] != 0 {
		t.Fatalf("a zero weight was still offered: %v", seen)
	}
	if seen[KindPut] == 0 || seen[KindGet] == 0 {
		t.Fatalf("a weighted kind was never offered: %v", seen)
	}
}

func TestParseMixRefusesNonsense(t *testing.T) {
	for _, text := range []string{"", "put", "put=x", "sideways=1", "put=0"} {
		if _, err := ParseMix(text); err == nil {
			t.Fatalf("%q was accepted", text)
		}
	}
	mix, err := ParseMix("put=15,get=55,cas=25,scan=5")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if mix != (Mix{Put: 15, Get: 55, CompareAndSwap: 25, Scan: 5}) {
		t.Fatalf("parsed %+v", mix)
	}
}

// An empty distribution says so rather than reporting zeros. The whole
// report rests on a reader being able to tell "nothing happened" from
// "nobody measured".
func TestAnEmptyDistributionIsAbsentAndNotZero(t *testing.T) {
	var samples Samples
	empty := samples.Percentiles()
	if empty.Observed != nil || empty.Absent != NoSamples {
		t.Fatalf("an empty distribution reported %+v", empty)
	}
	for i := 1; i <= 100; i++ {
		samples.Add(time.Duration(i) * time.Millisecond)
	}
	full := samples.Percentiles()
	if full.Observed == nil {
		t.Fatal("a hundred samples reported nothing")
	}
	if full.Observed.Count != 100 {
		t.Fatalf("count %d", full.Observed.Count)
	}
	if *full.Observed.MinNs != uint64(time.Millisecond) {
		t.Fatalf("min %d", *full.Observed.MinNs)
	}
	if *full.Observed.MaxNs != uint64(100*time.Millisecond) {
		t.Fatalf("max %d", *full.Observed.MaxNs)
	}
	if *full.Observed.P50Ns < uint64(45*time.Millisecond) ||
		*full.Observed.P50Ns > uint64(55*time.Millisecond) {
		t.Fatalf("p50 %d", *full.Observed.P50Ns)
	}
}

// A run with no watcher reports that no event delay was measured; it
// does not report that events were instant.
func TestEventDelayWithoutAWatcherIsAbsent(t *testing.T) {
	events := observedEvents(nil, map[int64]time.Time{1: time.Now()})
	if events.Delivered.Observed != nil || events.Delivered.Absent != NotStated {
		t.Fatalf("reported %+v", events.Delivered)
	}
}

// A write whose event never arrived is counted as missed. A watcher
// that is behind is a finding, and a report that quietly dropped the
// sample would hide it.
func TestAWriteWithNoEventIsMissedAndNotDropped(t *testing.T) {
	now := time.Now()
	watcher := &backendWatcher{at: map[int64]time.Time{2: now.Add(5 * time.Millisecond)}}
	events := observedEvents(watcher, map[int64]time.Time{1: now, 2: now})
	if events.Missed != 1 {
		t.Fatalf("missed %d", events.Missed)
	}
	if events.Observed != 1 {
		t.Fatalf("observed %d", events.Observed)
	}
}

func TestPrefixEndIsTheExclusiveUpperBound(t *testing.T) {
	if got := prefixEnd("/registry/bench/"); got != "/registry/bench0" {
		t.Fatalf("prefixEnd = %q", got)
	}
	if got := prefixEnd("\xff\xff"); got != "" {
		t.Fatalf("an all-0xff prefix has no upper bound, got %q", got)
	}
}
