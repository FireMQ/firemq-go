package firemq

import (
	"errors"
	"fmt"
)

// ErrClosed is returned, possibly wrapped, by every operation on a closed connection.
// Use errors.Is(err, firemq.ErrClosed) to detect it. When the connection closed because
// of a server error, errors.As also finds the *ServerError.
var ErrClosed = errors.New("firemq: connection closed")

// ErrCancelled is returned by Subscription.Next once the subscription has been
// cancelled, either by Subscription.Cancel or by the broker. When the broker cancelled
// it, for example because the queue was deleted, errors.As finds a *ServerError with
// the broker's code and reason.
var ErrCancelled = errors.New("firemq: subscription cancelled")

// ServerError means the broker refused a request. Code is an AMQP reply code such as
// 403 (access refused), 404 (not found) or 406 (precondition failed).
type ServerError struct {
	Code    uint16
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("firemq: %s (code %d)", e.Message, e.Code)
}

// UnroutableError means a mandatory publish reached no queue. The broker returned the
// message with code 312 (no-route).
type UnroutableError struct {
	Exchange   string
	RoutingKey string
}

func (e *UnroutableError) Error() string {
	return fmt.Sprintf("firemq: message returned: no queue is bound to exchange %q with key %q",
		e.Exchange, e.RoutingKey)
}

// NackedError means the broker rejected a publish, for example because a queue with
// x-overflow set to reject-publish is full.
type NackedError struct {
	Code    uint16
	Message string
}

func (e *NackedError) Error() string {
	return fmt.Sprintf("firemq: publish rejected: %s (code %d)", e.Message, e.Code)
}

// ErrorCode returns the AMQP reply code carried by err, or 0 when it has none. It
// understands *ServerError, *NackedError and *UnroutableError (312), also when wrapped.
func ErrorCode(err error) uint16 {
	var se *ServerError
	if errors.As(err, &se) {
		return se.Code
	}
	var ne *NackedError
	if errors.As(err, &ne) {
		return ne.Code
	}
	var ue *UnroutableError
	if errors.As(err, &ue) {
		return 312
	}
	return 0
}

// closedError is the error a closed connection reports. It matches ErrClosed and
// unwraps to the reason.
type closedError struct {
	reason error
}

func (e *closedError) Error() string {
	if e.reason == nil {
		return ErrClosed.Error()
	}
	return ErrClosed.Error() + ": " + e.reason.Error()
}

func (e *closedError) Is(target error) bool { return target == ErrClosed }

func (e *closedError) Unwrap() error { return e.reason }

func closedBecause(format string, args ...any) error {
	return &closedError{reason: fmt.Errorf(format, args...)}
}
