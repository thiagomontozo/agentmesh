package protocol_test

import (
	"errors"
	"testing"

	"github.com/thiagomontozo/agentmesh/internal/protocol"
)

func TestVersionCompatibility(t *testing.T) {
	if err := protocol.ValidateVersion(protocol.Version1); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"", "2", "v1", "1.0"} {
		err := protocol.ValidateVersion(version)
		if !errors.Is(err, protocol.ErrUnsupportedVersion) {
			t.Fatalf("version %q: expected typed unsupported error, got %v", version, err)
		}
		var versionError *protocol.UnsupportedVersionError
		if !errors.As(err, &versionError) || versionError.Received != version || len(versionError.Supported) != 1 || versionError.Supported[0] != protocol.Version1 {
			t.Fatalf("version %q: unexpected detail %+v", version, versionError)
		}
	}
}

func TestSupportedVersionsReturnsCopy(t *testing.T) {
	versions := protocol.SupportedVersions()
	versions[0] = "changed"
	if current := protocol.SupportedVersions(); current[0] != protocol.Version1 {
		t.Fatalf("caller mutated supported versions: %v", current)
	}
}
