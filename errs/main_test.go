package errs

import (
	"errors"
	"testing"
)

func TestErrorUnwrapTypedNil(t *testing.T) {
	var typedNil *Error
	var err error = typedNil

	if errors.Is(err, errors.New("unrelated")) {
		t.Fatal("typed-nil error unexpectedly matched")
	}
	if got := typedNil.Unwrap(); got != nil {
		t.Fatalf("Unwrap() = %v, want nil", got)
	}
}
