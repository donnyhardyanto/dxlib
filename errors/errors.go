// Package errors provides error handling with lazy stack trace support.
// Drop-in replacement for github.com/pkg/errors using only standard library.
//
// Stack traces are captured lazily:
// - Fast path: Only capture program counters ([]uintptr) when error is created
// - Slow path: Resolve to human-readable stack only when %+v is used
//
// Usage:
//
//	import "github.com/donnyhardyanto/dxlib/errors"
//
//	err := errors.New("something failed")
//	err := errors.Wrap(err, "context")
//	fmt.Printf("%+v\n", err)  // prints with stack trace
package errors

import (
	stderrors "errors"
	"fmt"
	"io"
	"runtime"
)

// ============================================================================
// Stack Types (compatible with github.com/pkg/errors)
// ============================================================================

// Frame represents a program counter inside a stack frame.
// For historical reasons if Frame is interpreted as a uintptr
// its value represents the program counter + 1.
type Frame uintptr

// pc returns the program counter for this frame;
// multiple frames may have the same PC value.
func (f Frame) pc() uintptr { return uintptr(f) - 1 }

// Format formats the frame according to the fmt.Formatter interface.
//
//	%s    source file
//	%d    source line
//	%n    function name
//	%v    equivalent to %s:%d
//
// Format accepts flags that alter the printing of some verbs, as follows:
//
//	%+s   function name and path of source file relative to the compile time
//	      GOPATH separated by \n\t (<funcname>\n\t<path>)
//	%+v   equivalent to %+s:%d
func (f Frame) Format(s fmt.State, verb rune) {
	pc := f.pc()
	fn := runtime.FuncForPC(pc)
	if fn == nil {
		io.WriteString(s, "unknown")
		return
	}

	switch verb {
	case 's':
		file, _ := fn.FileLine(pc)
		if s.Flag('+') {
			io.WriteString(s, fn.Name())
			io.WriteString(s, "\n\t")
			io.WriteString(s, file)
		} else {
			io.WriteString(s, file)
		}
	case 'd':
		_, line := fn.FileLine(pc)
		io.WriteString(s, fmt.Sprintf("%d", line))
	case 'n':
		io.WriteString(s, fn.Name())
	case 'v':
		f.Format(s, 's')
		io.WriteString(s, ":")
		f.Format(s, 'd')
	}
}

// StackTrace is a stack of Frames from innermost (newest) to outermost (oldest).
type StackTrace []Frame

// Format formats the stack of Frames according to the fmt.Formatter interface.
//
//	%s	lists source files for each Frame in the stack
//	%v	lists the source file and line number for each Frame in the stack
//
// Format accepts flags that alter the printing of some verbs, as follows:
//
//	%+v   Prints filename, function, and line number for each Frame in the stack.
func (st StackTrace) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		switch {
		case s.Flag('+'):
			for _, f := range st {
				io.WriteString(s, "\n")
				f.Format(s, verb)
			}
		case s.Flag('#'):
			fmt.Fprintf(s, "%#v", []Frame(st))
		default:
			st.formatSlice(s, verb)
		}
	case 's':
		st.formatSlice(s, verb)
	}
}

func (st StackTrace) formatSlice(s fmt.State, verb rune) {
	io.WriteString(s, "[")
	for i, f := range st {
		if i > 0 {
			io.WriteString(s, " ")
		}
		f.Format(s, verb)
	}
	io.WriteString(s, "]")
}

// ============================================================================
// Stack Capture (Fast Path)
// ============================================================================

const maxStackDepth = 32

// captureStack captures program counters only (very fast, no string allocation)
// skip: number of frames to skip (caller of captureStack + captureStack itself)
func captureStack(skip int) []uintptr {
	var pcs [maxStackDepth]uintptr
	n := runtime.Callers(skip, pcs[:])
	// Return a copy to avoid holding reference to the array
	stack := make([]uintptr, n)
	copy(stack, pcs[:n])
	return stack
}

// captureStackTrace returns a StackTrace ([]Frame) instead of []uintptr
func captureStackTrace(skip int) StackTrace {
	pcs := captureStack(skip + 1)
	st := make(StackTrace, len(pcs))
	for i, pc := range pcs {
		st[i] = Frame(pc)
	}
	return st
}

// ============================================================================
// Error Types
// ============================================================================

// stackError is an error with a captured stack trace
type stackError struct {
	msg   string
	cause error
	stack []uintptr // raw program counters, resolved lazily
}

func (e *stackError) Error() string {
	if e.cause != nil {
		if e.msg != "" {
			return e.msg + ": " + e.cause.Error()
		}
		return e.cause.Error()
	}
	return e.msg
}

func (e *stackError) Unwrap() error {
	return e.cause
}

// StackTrace returns the stack trace as a StackTrace type.
// This is compatible with github.com/pkg/errors.
func (e *stackError) StackTrace() StackTrace {
	st := make(StackTrace, len(e.stack))
	for i, pc := range e.stack {
		st[i] = Frame(pc)
	}
	return st
}

// Format implements fmt.Formatter for lazy stack resolution
// %s, %v: prints error message only (fast)
// %+v: prints error message + full stack trace (slow, only when needed)
func (e *stackError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			// Slow path: resolve stack trace only here
			io.WriteString(s, e.Error())
			frames := runtime.CallersFrames(e.stack)
			for {
				frame, more := frames.Next()
				if frame.Function == "" {
					break
				}
				fmt.Fprintf(s, "\n%s\n\t%s:%d", frame.Function, frame.File, frame.Line)
				if !more {
					break
				}
			}
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

// withMessage wraps error with message but no new stack trace
type withMessage struct {
	cause error
	msg   string
}

func (w *withMessage) Error() string {
	return w.msg + ": " + w.cause.Error()
}

func (w *withMessage) Unwrap() error {
	return w.cause
}

func (w *withMessage) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v\n", w.cause)
			io.WriteString(s, w.msg)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, w.Error())
	case 'q':
		fmt.Fprintf(s, "%q", w.Error())
	}
}

// ============================================================================
// Constructor Functions (Fast Path - only capture program counters)
// ============================================================================

// New creates error with message and captures stack trace.
// Stack is captured as raw pointers (fast), resolved only when %+v is used.
func New(message string) error {
	return &stackError{
		msg:   message,
		stack: captureStack(3), // skip: Callers, captureStack, New
	}
}

// Errorf creates formatted error with stack trace.
func Errorf(format string, args ...any) error {
	return &stackError{
		msg:   fmt.Sprintf(format, args...),
		stack: captureStack(3),
	}
}

// Wrap wraps error with message and captures stack trace.
// Returns nil if err is nil.
func Wrap(err error, message string) error {
	if err == nil {
		return nil
	}
	return &stackError{
		msg:   message,
		cause: err,
		stack: captureStack(3),
	}
}

// Wrapf wraps error with formatted message and captures stack trace.
// Returns nil if err is nil.
func Wrapf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return &stackError{
		msg:   fmt.Sprintf(format, args...),
		cause: err,
		stack: captureStack(3),
	}
}

// WithStack adds stack trace to existing error without additional message.
// Returns nil if err is nil.
func WithStack(err error) error {
	if err == nil {
		return nil
	}
	return &stackError{
		cause: err,
		stack: captureStack(3),
	}
}

// WithMessage wraps error with message but NO new stack trace.
// Use this when you want to add context without capturing a new stack.
// Returns nil if err is nil.
func WithMessage(err error, message string) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   message,
	}
}

// WithMessagef wraps error with formatted message but NO new stack trace.
// Returns nil if err is nil.
func WithMessagef(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	return &withMessage{
		cause: err,
		msg:   fmt.Sprintf(format, args...),
	}
}

// ============================================================================
// Utility Functions
// ============================================================================

// Cause returns the root cause of error by unwrapping all layers.
func Cause(err error) error {
	for err != nil {
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			break
		}
		unwrapped := unwrapper.Unwrap()
		if unwrapped == nil {
			break
		}
		err = unwrapped
	}
	return err
}

// ============================================================================
// Re-exports from standard library
// ============================================================================

var (
	Is     = stderrors.Is
	As     = stderrors.As
	Unwrap = stderrors.Unwrap
	Join   = stderrors.Join
)
