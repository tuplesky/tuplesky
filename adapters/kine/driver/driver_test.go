package driver

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
	kinetls "github.com/k3s-io/kine/pkg/tls"
	"github.com/tuplesky/tuplesky/adapters/kine/internal/testpki"
)

var goodDSN = "coord://frontend.local:4433?cluster=" + strings.Repeat("01", 16) +
	"&domain=" + "000102030405060708090a0b0c0d0e0f" +
	"&namespace=" + strings.Repeat("03", 16) +
	"&assertion=/var/run/secrets/tuplesky/assertion&sts=https://sts.local/v1/token&audience=cluster-resource"

// Importing the package registers the scheme in Kine's driver registry.
func TestSchemeIsRegistered(t *testing.T) {
	if _, ok := drivers.Get(Scheme); !ok {
		t.Fatal("coord scheme not registered")
	}
	if _, ok := drivers.Get("sqlite"); ok {
		t.Fatal("this build links a SQL driver")
	}
}

func TestParseDSN(t *testing.T) {
	d, err := ParseDSN(goodDSN)
	if err != nil {
		t.Fatal(err)
	}
	if d.Frontend != "frontend.local:4433" || d.ServerName != "frontend.local" || d.Domain[1] != 1 || d.Domain[15] != 0x0f || d.Namespace[0] != 3 {
		t.Fatalf("%+v", d)
	}
	if d.Audience != "cluster-resource" || d.STSURL != "https://sts.local/v1/token" || d.DeadlineMs != 10_000 || d.Session != nil {
		t.Fatalf("%+v", d)
	}
	// Kine hands the driver the DSN without its scheme.
	stripped, err := ParseDSN(strings.TrimPrefix(goodDSN, "coord://") + "&server-name=fe.internal&deadline-ms=500&session=" + strings.Repeat("05", 16))
	if err != nil || stripped.ServerName != "fe.internal" || stripped.DeadlineMs != 500 || stripped.Session == nil || stripped.Session[0] != 5 {
		t.Fatalf("%v %+v", err, stripped)
	}
	// A random instance per process unless pinned.
	if d.ClientInstance == stripped.ClientInstance {
		t.Fatal("instances collide")
	}
	bad := map[string]error{
		"coord://?cluster=01":                                                                   ErrInvalidDSN,
		"coord://frontend.local?cluster=01":                                                     ErrInvalidDSN,
		"coord://user@frontend.local:1?cluster=01":                                              ErrInvalidDSN,
		goodDSN + "&insecure=true":                                                              ErrInsecureOption,
		goodDSN + "&skip-verify=1":                                                              ErrInsecureOption,
		goodDSN + "&unknown=1":                                                                  ErrInvalidDSN,
		strings.Replace(goodDSN, "https://", "http://", 1):                                      ErrInsecureOption,
		strings.Replace(goodDSN, "&audience=cluster-resource", "", 1):                           ErrInvalidDSN,
		strings.Replace(goodDSN, strings.Repeat("03", 16), "zz", 1):                             ErrInvalidDSN,
		strings.Replace(goodDSN, strings.Repeat("03", 16), "03", 1):                             ErrInvalidDSN,
		goodDSN + "&domain=" + strings.Repeat("02", 16) + "&domain=" + strings.Repeat("04", 16): nil,
	}
	for dsn, want := range bad {
		_, err := ParseDSN(dsn)
		if want == nil {
			// Two domains: url.Values keeps the first; the backend still
			// binds exactly one domain (the DSN's first). Documented here
			// so a reviewer sees the choice.
			continue
		}
		if !errors.Is(err, want) {
			t.Fatalf("%s: %v, want %v", dsn, err, want)
		}
	}
}

// The registered constructor refuses a disabled verifier and a missing
// trust root, and builds an unstarted backend otherwise.
func TestConstructorRequiresVerifiedFrontendIdentity(t *testing.T) {
	ca, err := testpki.NewCA("fe-ca")
	if err != nil {
		t.Fatal(err)
	}
	caPath, err := ca.WriteCA(t.TempDir(), "ca")
	if err != nil {
		t.Fatal(err)
	}
	wg := &sync.WaitGroup{}
	_, _, err = drivers.New(context.Background(), wg, &drivers.Config{Endpoint: goodDSN, BackendTLSConfig: kinetls.Config{CAFile: caPath, SkipVerify: true}})
	if !errors.Is(err, ErrInsecureOption) {
		t.Fatalf("skip-verify: %v", err)
	}
	_, _, err = drivers.New(context.Background(), wg, &drivers.Config{Endpoint: goodDSN})
	if !errors.Is(err, ErrNoTrustRoot) {
		t.Fatalf("no CA: %v", err)
	}
	_, _, err = drivers.New(context.Background(), wg, &drivers.Config{Endpoint: "coord://frontend.local:1?bogus=1", BackendTLSConfig: kinetls.Config{CAFile: caPath}})
	if !errors.Is(err, ErrInvalidDSN) {
		t.Fatalf("bad DSN: %v", err)
	}
	if _, _, err := drivers.New(context.Background(), wg, &drivers.Config{Endpoint: "sqlite://x"}); !errors.Is(err, drivers.ErrUnknownDriver) {
		t.Fatalf("unregistered scheme: %v", err)
	}
	leader, be, err := drivers.New(context.Background(), wg, &drivers.Config{Endpoint: goodDSN, BackendTLSConfig: kinetls.Config{CAFile: caPath}})
	if err != nil || leader || be == nil {
		t.Fatalf("%v %v %v", err, leader, be)
	}
}
