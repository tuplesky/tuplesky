package wire

import "testing"

func TestSchemaFamilyIsFrozen(t *testing.T) {
	if SchemaFamily != "wire_v1" {
		t.Fatalf("schema family drifted: %q", SchemaFamily)
	}
}
