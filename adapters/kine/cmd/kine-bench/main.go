// kine-bench offers one shaped workload down one composition of a
// provisioned domain and reports what it cost (task-63).
//
//	kine-bench -harness RUN/harness.json -arm edge   -out edge.json
//	kine-bench -harness RUN/harness.json -arm backend \
//	    -dsn "$(coord-harness dsn --dir RUN)" -ca-file RUN/roots.pem -out backend.json
//
// The two arms are the same domain seen from two places: `edge` is what
// an API server drives, `backend` is the same coord:// backend with the
// etcd edge taken away. Neither is a claim about the other, and the
// difference between them is the edge's cost only when both were
// offered the same work against the same domain in the same run --
// which is what `scripts/bench/kine-overhead.sh` arranges.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/tuplesky/tuplesky/adapters/kine/bench"
)

// harness is the part of a provisioned domain this program reads.
type harness struct {
	Voters []struct {
		API string `json:"api"`
	} `json:"voters"`
	Edge struct {
		Endpoint          string `json:"endpoint"`
		ServerName        string `json:"server_name"`
		ServerCA          string `json:"server_ca"`
		ClientCertificate string `json:"client_certificate"`
		ClientKey         string `json:"client_key"`
	} `json:"edge"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "kine-bench:", err)
		os.Exit(1)
	}
}

func run() error {
	harnessPath := flag.String("harness", "", "the provisioned domain's harness.json")
	arm := flag.String("arm", "edge", "edge (an API server's path) or backend (the same backend without the etcd edge)")
	dsn := flag.String("dsn", "", "backend arm: the coord:// DSN (never logged)")
	caFile := flag.String("ca-file", "", "backend arm: the CA that issued the frontend's TLS identity")
	label := flag.String("label", "", "the operator's label for this run")
	durability := flag.String("durability", "", "what the domain is running under, in your own words; required")
	topology := flag.String("topology", "", "the shape of the domain, as you set it up")
	impairment := flag.String("impairment", "", "the delay and loss you applied, if any; omitted means it was not stated, never that it was none")
	seed := flag.Uint64("seed", 1, "workload seed")
	arrivalNs := flag.Uint64("arrival-ns", 0, "scheduled inter-arrival time; 0 is a closed loop")
	warmupOps := flag.Uint64("warmup-ops", 200, "operations offered before measurement starts")
	measuredOps := flag.Uint64("measured-ops", 2000, "operations measured")
	callers := flag.Int("callers", 8, "concurrent callers, each with its own connection")
	deadlineMs := flag.Uint64("deadline-ms", 10000, "per-operation deadline")
	mix := flag.String("mix", "put=15,get=55,cas=25,scan=5", "relative weights")
	prefix := flag.String("prefix", "/registry/bench/", "the registry prefix every key sits under")
	keyspace := flag.Uint64("keyspace", 4096, "distinct keys the reads and writes touch")
	hotKeys := flag.Uint64("hot-keys", 64, "keys the guarded writes contend on")
	valueBytes := flag.Int("value-bytes", 512, "value size")
	scanLimit := flag.Int64("scan-limit", 64, "rows a scan asks for")
	observe := flag.Bool("observe-events", true, "watch beside the callers and report event delay separately from acknowledgement")
	out := flag.String("out", "", "where to write the report")
	flag.Parse()

	if *harnessPath == "" {
		return fmt.Errorf("-harness is required")
	}
	raw, err := os.ReadFile(*harnessPath)
	if err != nil {
		return err
	}
	var h harness
	if err := json.Unmarshal(raw, &h); err != nil {
		return fmt.Errorf("parse %s: %w", *harnessPath, err)
	}
	parsedMix, err := bench.ParseMix(*mix)
	if err != nil {
		return err
	}
	deadline := time.Duration(*deadlineMs) * time.Millisecond

	var driven bench.Arm
	switch *arm {
	case "edge":
		if h.Edge.Endpoint == "" {
			return fmt.Errorf("%s names no storage edge", *harnessPath)
		}
		driven, err = bench.NewEdgeArm(bench.EdgeConfig{
			Endpoint:          h.Edge.Endpoint,
			ServerName:        h.Edge.ServerName,
			ClientCertificate: h.Edge.ClientCertificate,
			ClientKey:         h.Edge.ClientKey,
			ServerCA:          h.Edge.ServerCA,
			Deadline:          deadline,
		})
		if err != nil {
			return err
		}
	case "backend":
		if *dsn == "" || *caFile == "" {
			return fmt.Errorf("the backend arm needs -dsn and -ca-file")
		}
		driven = bench.NewBackendArm(*dsn, *caFile)
	default:
		return fmt.Errorf("unknown arm %q: edge or backend", *arm)
	}
	defer driven.Close()

	name := *label
	if name == "" {
		name = fmt.Sprintf("%s (%d callers, %dns)", *arm, *callers, *arrivalNs)
	}
	endpoint := h.Edge.Endpoint
	if *arm == "backend" && len(h.Voters) > 0 {
		endpoint = h.Voters[0].API
	}
	report, err := bench.Run(context.Background(), driven, bench.Spec{
		Label:      name,
		Seed:       *seed,
		Durability: *durability,
		Topology: bench.Topology{
			Label:      *topology,
			Voters:     len(h.Voters),
			Endpoint:   endpoint,
			Impairment: *impairment,
		},
		ArrivalNs:     *arrivalNs,
		WarmupOps:     *warmupOps,
		MeasuredOps:   *measuredOps,
		Callers:       *callers,
		Deadline:      deadline,
		ObserveEvents: *observe,
		Workload: bench.Workload{
			Prefix:     *prefix,
			Keyspace:   uint32(*keyspace),
			HotKeys:    uint32(*hotKeys),
			ValueBytes: *valueBytes,
			ScanLimit:  *scanLimit,
			Mix:        parsedMix,
		},
	})
	if err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	if *out == "" {
		fmt.Println(string(encoded))
		return nil
	}
	return os.WriteFile(*out, append(encoded, '\n'), 0o600)
}
