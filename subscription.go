package firemq

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// credit tracks a subscription's credit window and grants credit back to the broker in
// batches of half the window as messages are handled.
type credit struct {
	client *Client
	sub    uint32
	window uint32
	active atomic.Bool

	mu       sync.Mutex
	released uint32
}

func newCredit(c *Client, sub, window uint32) *credit {
	cr := &credit{client: c, sub: sub, window: window}
	cr.active.Store(true)
	return cr
}

// release records n handled messages and grants them back once at least half the
// window has been handled.
func (cr *credit) release(n uint32) {
	batch := max(cr.window/2, 1)
	cr.mu.Lock()
	cr.released += n
	if cr.released < batch {
		cr.mu.Unlock()
		return
	}
	granted := cr.released
	cr.released = 0
	cr.mu.Unlock()
	if cr.active.Load() {
		_ = cr.client.send(opCredit, &creditHeader{Sub: cr.sub, Credit: granted}, nil)
	}
}

// Subscription delivers messages from a queue. Read them from Messages or with Next.
// A Subscription is safe for concurrent use.
type Subscription struct {
	client *Client
	id     uint32
	queue  string
	noAck  bool
	credit *credit

	out        chan *Message
	stop       chan struct{} // closed to stop the feeder
	feederDone chan struct{} // closed when the feeder exits
	wake       chan struct{}
	stopOnce   sync.Once

	mu              sync.Mutex
	pending         []*Message // received from the broker, not yet handed to the app
	ended           bool       // no more messages will be added to pending
	serverCancelled bool
	err             error // why the subscription ended
}

func newSubscription(c *Client, id uint32, queue string, window uint32, noAck bool) *Subscription {
	return &Subscription{
		client:     c,
		id:         id,
		queue:      queue,
		noAck:      noAck,
		credit:     newCredit(c, id, window),
		out:        make(chan *Message),
		stop:       make(chan struct{}),
		feederDone: make(chan struct{}),
		wake:       make(chan struct{}, 1),
	}
}

// Queue returns the name of the queue this subscription consumes from.
func (s *Subscription) Queue() string { return s.queue }

// Messages returns the channel messages arrive on. It is closed when the subscription
// ends: after Cancel, when the broker cancels the subscription, or when the connection
// closes. Err then reports why.
//
// Unless the subscription uses NoAck, acknowledge every message with Ack or Nack: the
// broker stops sending once Prefetch messages are unacknowledged.
func (s *Subscription) Messages() <-chan *Message { return s.out }

// Next waits for the next message. Once the subscription has ended it returns Err:
// an error matching ErrCancelled or ErrClosed.
func (s *Subscription) Next(ctx context.Context) (*Message, error) {
	select {
	case m, ok := <-s.out:
		if !ok {
			return nil, s.Err()
		}
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Err returns why the subscription ended, or nil while it is active. The error matches
// ErrCancelled when the subscription was cancelled and ErrClosed when the connection
// closed.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Cancel stops the subscription and waits for the broker to confirm. Messages already
// received but not yet read from Messages go back to the queue. Messages the
// application already holds can still be acknowledged. Cancel on an ended
// subscription returns nil.
func (s *Subscription) Cancel(ctx context.Context) error {
	first := false
	s.stopOnce.Do(func() { first = true })
	if !first {
		return nil
	}
	c := s.client
	s.credit.active.Store(false)
	s.mu.Lock()
	if s.err == nil {
		s.err = ErrCancelled
	}
	serverCancelled := s.serverCancelled
	s.mu.Unlock()
	close(s.stop)
	<-s.feederDone
	if serverCancelled || c.IsClosed() {
		s.end(ErrCancelled)
		return nil
	}

	rid, ch, err := c.start(opUnsubscribe, func(rid uint32) any {
		return &unsubscribeHeader{Rid: rid, Sub: s.id}
	})
	// Deliveries that arrive from now on are requeued by the reader. Requeue the ones
	// already buffered too; the broker sees these nacks after the unsubscribe.
	for _, m := range s.end(ErrCancelled) {
		_ = m.Nack(true)
	}
	if err == nil {
		_, err = c.await(ctx, rid, ch)
	}
	c.removeSub(s.id, s)
	if errors.Is(err, ErrClosed) {
		return nil
	}
	return err
}

// push adds a delivery from the broker. It reports false when the subscription no
// longer accepts messages.
func (s *Subscription) push(m *Message) bool {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return false
	}
	s.pending = append(s.pending, m)
	s.mu.Unlock()
	s.signal()
	return true
}

func (s *Subscription) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// end stops accepting deliveries, records reason unless an earlier one exists, and
// returns the messages that were not handed to the application.
func (s *Subscription) end(reason error) []*Message {
	s.mu.Lock()
	if s.err == nil {
		s.err = reason
	}
	s.ended = true
	rest := s.pending
	s.pending = nil
	s.mu.Unlock()
	s.signal()
	return rest
}

// cancelledByServer ends the subscription after a cancelled frame. Buffered messages
// are still handed to the application before Messages closes.
func (s *Subscription) cancelledByServer(reason error) {
	s.credit.active.Store(false)
	s.mu.Lock()
	if s.err == nil {
		s.err = reason
	}
	s.ended = true
	s.serverCancelled = true
	s.mu.Unlock()
	s.signal()
}

// run hands buffered messages to the application one at a time, so that NoAck
// subscriptions can grant credit as the application takes them.
func (s *Subscription) run() {
	defer s.client.wg.Done()
	defer close(s.out)
	defer close(s.feederDone)
	for {
		s.mu.Lock()
		var m *Message
		if len(s.pending) > 0 {
			m = s.pending[0]
			s.pending[0] = nil
			s.pending = s.pending[1:]
		}
		ended := s.ended
		s.mu.Unlock()
		if m == nil {
			if ended {
				return
			}
			select {
			case <-s.wake:
				continue
			case <-s.stop:
				return
			case <-s.client.done:
				return
			}
		}
		select {
		case s.out <- m:
			if s.noAck {
				s.credit.release(1)
			}
		case <-s.stop:
			// Put it back so Cancel can requeue it.
			s.mu.Lock()
			s.pending = append([]*Message{m}, s.pending...)
			s.mu.Unlock()
			return
		case <-s.client.done:
			return
		}
	}
}
