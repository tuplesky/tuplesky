package pin

import "testing"

func TestPinnedVersionIsRFC9000(t *testing.T) {
	if uint32(QUICVersion) != 0x1 {
		t.Fatalf("expected QUIC v1, got %#x", uint32(QUICVersion))
	}
}
