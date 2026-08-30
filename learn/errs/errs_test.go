package errs_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/keshavsaini0311/raftkv/learn/errs"
)

func TestFetch(t *testing.T) {
	store := map[string]string{"term": "7"}

	t.Run("hit", func(t *testing.T) {
		got, err := errs.Fetch(store, "term")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "7" {
			t.Errorf("Fetch = %q, want %q", got, "7")
		}
	})

	t.Run("miss returns the sentinel", func(t *testing.T) {
		got, err := errs.Fetch(store, "absent")
		if !errors.Is(err, errs.ErrNotFound) {
			t.Errorf("err = %v, want ErrNotFound", err)
		}
		if got != "" {
			t.Errorf("on error the value should be the zero string, got %q", got)
		}
	})
}

// The point of %w: context is added, and the sentinel survives underneath.
func TestLoadConfigWraps(t *testing.T) {
	store := map[string]string{"good": "yes"}

	t.Run("success", func(t *testing.T) {
		got, err := errs.LoadConfig(store, "good")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "yes" {
			t.Errorf("got %q, want %q", got, "yes")
		}
	})

	t.Run("failure keeps the sentinel reachable", func(t *testing.T) {
		_, err := errs.LoadConfig(store, "missing")
		if err == nil {
			t.Fatal("expected an error")
		}

		// This is what %w buys. With %v it would fail.
		if !errors.Is(err, errs.ErrNotFound) {
			t.Errorf("errors.Is(err, ErrNotFound) = false for %v; "+
				"did you use %%v instead of %%w?", err)
		}

		// And the context was actually added.
		if !strings.Contains(err.Error(), "missing") {
			t.Errorf("err = %q, want it to mention the key name", err)
		}
		if err.Error() == errs.ErrNotFound.Error() {
			t.Error("err adds no context; it is just the bare sentinel")
		}
	})
}

func TestValidate(t *testing.T) {
	t.Run("valid input returns nil", func(t *testing.T) {
		if err := errs.Validate("keshav"); err != nil {
			t.Errorf("Validate = %v, want nil", err)
		}
	})

	t.Run("invalid input carries its details", func(t *testing.T) {
		err := errs.Validate("")
		if err == nil {
			t.Fatal("expected an error")
		}

		// errors.As unwraps looking for a TYPE, then fills in your variable.
		var ve *errs.ValidationError
		if !errors.As(err, &ve) {
			t.Fatalf("errors.As did not match *ValidationError for %v", err)
		}
		if ve.Field != "Name" {
			t.Errorf("Field = %q, want %q", ve.Field, "Name")
		}
		if ve.Reason != "must not be empty" {
			t.Errorf("Reason = %q, want %q", ve.Reason, "must not be empty")
		}

		want := `invalid field "Name": must not be empty`
		if err.Error() != want {
			t.Errorf("Error() = %q, want %q", err.Error(), want)
		}
	})
}

// The trap, demonstrated. BrokenValidate is written to be wrong on purpose.
func TestBrokenValidateReturnsNonNilOnSuccess(t *testing.T) {
	err := errs.BrokenValidate("keshav") // valid input — should be no error

	if err == nil {
		t.Skip("BrokenValidate was fixed; this test exists to show the trap")
	}

	// It is not nil, yet there is no actual error inside it.
	var ve *errs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatal("expected the interface to hold a *ValidationError")
	}
	if ve != nil {
		t.Fatal("expected the held pointer itself to be nil")
	}

	t.Log("err != nil is true, but the *ValidationError inside it is nil — " +
		"every `if err != nil` caller now reports a failure that did not happen")
}

// Contrast: Validate returns a literal nil, so the interface is genuinely nil.
func TestValidateReturnsTrueNil(t *testing.T) {
	if err := errs.Validate("fine"); err != nil {
		t.Errorf("Validate returned %v; it must return a literal nil so the "+
			"interface has no type half either", err)
	}
}
