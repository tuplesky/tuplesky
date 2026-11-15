// Package wire will hold the restricted postcard subset and shared fixtures
// for the Kine adapter (task-44). task-01 establishes only the module
// boundary; no Serde reflection, cgo or protobuf-on-wire is introduced here.
package wire

// SchemaFamily names the frozen frame family this package will decode.
// The numeric kinds and versions are owned by the Rust wire_v1 schema
// (task-03) and mirrored here only through reviewed fixtures.
const SchemaFamily = "wire_v1"
