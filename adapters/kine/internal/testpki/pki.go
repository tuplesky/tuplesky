// Package testpki issues throwaway certificates for the edge tests: a CA,
// server identities and client identities with chosen names and
// validity. Nothing here is a production issuer.
package testpki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// CA is a test certificate authority.
type CA struct {
	Cert *x509.Certificate
	Key  *ecdsa.PrivateKey
	// PEM is the CA certificate.
	PEM []byte
}

// NewCA makes a CA.
func NewCA(name string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, Key: key, PEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})}, nil
}

// Issued is a leaf identity.
type Issued struct {
	TLS     tls.Certificate
	CertPEM []byte
	KeyPEM  []byte
}

// Issue signs a leaf with the DNS name; `server` selects the extended key
// usage; `notAfter` sets validity (a past instant issues an expired
// certificate).
func (ca *CA) Issue(name string, server bool, notAfter time.Time) (*Issued, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	usage := x509.ExtKeyUsageClientAuth
	if server {
		usage = x509.ExtKeyUsageServerAuth
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    notAfter.Add(-2 * time.Hour),
		NotAfter:     notAfter,
		DNSNames:     []string{name},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if server {
		// Loopback edges are dialled by address.
		tmpl.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return nil, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	c, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &Issued{TLS: c, CertPEM: certPEM, KeyPEM: keyPEM}, nil
}

// WriteFiles writes the PEMs into dir and returns their paths.
func (i *Issued) WriteFiles(dir, base string) (certPath, keyPath string, err error) {
	certPath = filepath.Join(dir, base+".crt")
	keyPath = filepath.Join(dir, base+".key")
	if err := os.WriteFile(certPath, i.CertPEM, 0o600); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyPath, i.KeyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certPath, keyPath, nil
}

// WriteCA writes the CA PEM into dir.
func (ca *CA) WriteCA(dir, base string) (string, error) {
	p := filepath.Join(dir, base+".crt")
	return p, os.WriteFile(p, ca.PEM, 0o600)
}

// Pool trusts the CA.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	return pool
}
