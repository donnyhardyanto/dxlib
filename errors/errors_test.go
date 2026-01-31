package errors

import (
	stderrors "errors"
	"fmt"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	err := New("test error")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "test error" {
		t.Errorf("expected 'test error', got %q", err.Error())
	}

	// Check stack trace with %+v
	stack := fmt.Sprintf("%+v", err)
	if !strings.Contains(stack, "TestNew") {
		t.Errorf("expected stack to contain 'TestNew', got:\n%s", stack)
	}
}

func TestErrorf(t *testing.T) {
	err := Errorf("error %d: %s", 42, "test")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "error 42: test" {
		t.Errorf("expected 'error 42: test', got %q", err.Error())
	}
}

func TestWrap(t *testing.T) {
	cause := stderrors.New("root cause")
	err := Wrap(cause, "wrapped")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "wrapped: root cause" {
		t.Errorf("expected 'wrapped: root cause', got %q", err.Error())
	}

	// Test nil case
	if Wrap(nil, "message") != nil {
		t.Error("expected nil for Wrap(nil, ...)")
	}
}

func TestWrapf(t *testing.T) {
	cause := stderrors.New("root cause")
	err := Wrapf(cause, "wrapped %d", 42)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "wrapped 42: root cause" {
		t.Errorf("expected 'wrapped 42: root cause', got %q", err.Error())
	}
}

func TestWithStack(t *testing.T) {
	cause := stderrors.New("original error")
	err := WithStack(cause)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != "original error" {
		t.Errorf("expected 'original error', got %q", err.Error())
	}

	// Check stack trace
	stack := fmt.Sprintf("%+v", err)
	if !strings.Contains(stack, "TestWithStack") {
		t.Errorf("expected stack to contain 'TestWithStack', got:\n%s", stack)
	}
}

func TestCause(t *testing.T) {
	root := stderrors.New("root")
	wrapped1 := Wrap(root, "layer1")
	wrapped2 := Wrap(wrapped1, "layer2")

	cause := Cause(wrapped2)
	if cause != root {
		t.Errorf("expected root cause, got %v", cause)
	}
}

func TestUnwrap(t *testing.T) {
	root := stderrors.New("root")
	wrapped := Wrap(root, "wrapped")

	unwrapped := Unwrap(wrapped)
	if unwrapped != root {
		t.Errorf("expected root, got %v", unwrapped)
	}
}

func TestIs(t *testing.T) {
	root := stderrors.New("root")
	wrapped := Wrap(root, "wrapped")

	if !Is(wrapped, root) {
		t.Error("expected Is to return true")
	}
}

type customError struct {
	Code int
}

func (e *customError) Error() string {
	return fmt.Sprintf("custom error: %d", e.Code)
}

func TestAs(t *testing.T) {
	custom := &customError{Code: 42}

	// Wrap with our package
	wrapped := Wrap(custom, "wrapped custom")

	var target *customError
	if !As(wrapped, &target) {
		t.Error("expected As to return true")
	}
	if target.Code != 42 {
		t.Errorf("expected Code=42, got %d", target.Code)
	}
}

func TestStackTraceFormat(t *testing.T) {
	err := helperFunction()
	stack := fmt.Sprintf("%+v", err)

	// Should contain the function names in the stack
	if !strings.Contains(stack, "helperFunction") {
		t.Errorf("expected stack to contain 'helperFunction':\n%s", stack)
	}
	if !strings.Contains(stack, "TestStackTraceFormat") {
		t.Errorf("expected stack to contain 'TestStackTraceFormat':\n%s", stack)
	}
}

func helperFunction() error {
	return New("error from helper")
}

func TestWithMessage(t *testing.T) {
	root := New("root")
	wrapped := WithMessage(root, "context")

	if wrapped.Error() != "context: root" {
		t.Errorf("expected 'context: root', got %q", wrapped.Error())
	}

	// WithMessage should not add new stack, but preserve original
	stack := fmt.Sprintf("%+v", wrapped)
	if !strings.Contains(stack, "root") {
		t.Errorf("expected stack to contain original error:\n%s", stack)
	}
}
