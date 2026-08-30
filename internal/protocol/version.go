// Package protocol defines compatibility rules shared by concrete Agent
// Protocol versions. Wire schemas remain in versioned subpackages such as v1.
package protocol

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	Version1                = "1"
	HeaderVersion           = "Agent-Protocol-Version"
	HeaderEffectIdempotency = "Agent-Effect-Idempotency-Key"
	CodeUnsupportedVersion  = "unsupported_protocol_version"
	CodeEffectIdempotency   = "effect_idempotency_not_acknowledged"
)

var ErrUnsupportedVersion = errors.New("unsupported Agent Protocol version")

var supportedVersions = []string{Version1}

type UnsupportedVersionError struct {
	Received  string
	Supported []string
}

func (e *UnsupportedVersionError) Error() string {
	return fmt.Sprintf("%s %q (supported: %s)", ErrUnsupportedVersion, e.Received, strings.Join(e.Supported, ", "))
}

func (e *UnsupportedVersionError) Unwrap() error { return ErrUnsupportedVersion }

func ValidateVersion(version string) error {
	if slices.Contains(supportedVersions, version) {
		return nil
	}
	return &UnsupportedVersionError{Received: version, Supported: SupportedVersions()}
}

func SupportedVersions() []string { return slices.Clone(supportedVersions) }
