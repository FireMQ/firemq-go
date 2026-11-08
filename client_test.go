package firemq

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeServer is a minimal in-process broker that accepts one connection, completes the
// handshake and lets the test script the rest.
type fakeServer struct {
	t     *testing.T
	ln    net.Listener
	conns chan *fakeConn
	url   string
}

type fakeConn struct {
	t     *testing.T
	conn  net.Conn
	r     *bufio.Reader
	hello *helloHeader
}

func newFakeServer(t *testing.T, welcome welcomeHeader) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeServer{t: t, ln: ln, conns: make(chan *fakeConn, 1), url: "fire://" + ln.Addr().String()}
	t.Cleanup(func() { ln.Close() })
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		fc := &fakeConn{t: t, conn: conn, r: bufio.NewReader(conn)}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		var m [4]byte
		if _, err := io.ReadFull(fc.r, m[:]); err != nil || string(m[:]) != "FMQ1" {
			conn.Close()
			return
		}
		f, err := readFrame(fc.r, maxHelloFrame)
		if err != nil {
			conn.Close()
			return
		}
		h, err := decodeCommand(f)
		if err != nil {
			conn.Close()
			return
		}
		fc.hello = h.(*helloHeader)
		if welcome.Ver == 0 {
			fc.writeErr(opError, &errorHeader{Code: 403, Msg: "ACCESS_REFUSED", Fatal: true})
			conn.Close()
			return
		}
		fc.writeErr(opWelcome, &welcome)
		s.conns <- fc
	}()
	return s
}

func (s *fakeServer) accept() *fakeConn {
	s.t.Helper()
	select {
	case fc := <-s.conns:
		s.t.Cleanup(func() { fc.conn.Close() })
		return fc
	case <-time.After(5 * time.Second):
		s.t.Fatal("no connection")
		return nil
	}
}

func (fc *fakeConn) writeErr(op byte, header any) error {
	return fc.writeFrame(op, header, nil)
}

func (fc *fakeConn) writeFrame(op byte, header any, payload []byte) error {
	b, err := encodeFrame(op, header, payload)
	if err != nil {
		return err
	}
	_, err = fc.conn.Write(b)
	return err
}

func (fc *fakeConn) write(op byte, header any, payload []byte) {
	fc.t.Helper()
	if err := fc.writeFrame(op, header, payload); err != nil {
		fc.t.Fatalf("writing %s: %v", opName(op), err)
	}
}

// next reads the next frame, skipping pings.
func (fc *fakeConn) next() (byte, any) {
	fc.t.Helper()
	for {
		fc.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		f, err := readFrame(fc.r, defaultMaxFrame)
		if err != nil {
			fc.t.Fatalf("reading a frame: %v", err)
		}
		if f.op == opPing {
			continue
		}
		h, err := decodeCommand(f)
		if err != nil {
			fc.t.Fatal(err)
		}
		return f.op, h
	}
}

func (fc *fakeConn) expect(op byte) any {
	fc.t.Helper()
	got, h := fc.next()
	if got != op {
		fc.t.Fatalf("got %s %+v, want %s", opName(got), h, opName(op))
	}
	return h
}

var defaultWelcome = welcomeHeader{Ver: 1, Server: serverInfoHeader{Name: "fake", Version: "0"}, MaxFrame: defaultMaxFrame}

func connectFake(t *testing.T, welcome welcomeHeader) (*Client, *fakeConn) {
	t.Helper()
	srv := newFakeServer(t, welcome)
	c, err := Connect(context.Background(), srv.url)
	if err != nil {
		t.Fatal(err)
	}
	fc := srv.accept()
	t.Cleanup(func() {
		// The fake broker does not answer close; do not wait for it.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()
		c.Close(ctx)
	})
	return c, fc
}

func waitDone(t *testing.T, c *Client) {
	t.Helper()
	select {
	case <-c.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not close")
	}
}

func TestHandshake(t *testing.T) {
	welcome := defaultWelcome
	welcome.ConnID = "conn-1"
	welcome.Heartbeat = 15
	welcome.Caps = []string{"confirms"}
	c, fc := connectFake(t, welcome)
	h := fc.hello
	if h.Ver != 1 || h.Vhost != "/" || h.Auth != (authHeader{Mech: "plain", User: "guest", Pass: "guest"}) || h.Heartbeat != 30 {
		t.Fatalf("hello = %+v", h)
	}
	if h.Client["name"] != "firemq-go" || h.Client["lang"] != "go" {
		t.Fatalf("client info = %v", h.Client)
	}
	s := c.Server()
	if s.Name != "fake" || s.ConnID != "conn-1" || s.Heartbeat != 15*time.Second || s.MaxFrame != defaultMaxFrame {
		t.Fatalf("server = %+v", s)
	}
}

func TestHandshakeRefused(t *testing.T) {
	srv := newFakeServer(t, welcomeHeader{})
	_, err := Connect(context.Background(), srv.url)
	var se *ServerError
	if !errors.As(err, &se) || se.Code != 403 {
		t.Fatalf("err = %v, want a 403 ServerError", err)
	}
}

func TestRequestsGetTheirReplies(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	qerr, xerr := make(chan error, 1), make(chan error, 1)
	var info QueueInfo
	go func() {
		var err error
		info, err = c.DeclareQueue(context.Background(), "", TemporaryQueue())
		qerr <- err
	}()
	dq := fc.expect(opDeclareQueue).(*declareQueueHeader)
	if dq.Durable || !dq.Exclusive || !dq.AutoDelete {
		t.Fatalf("declare_queue = %+v", dq)
	}
	go func() { xerr <- c.DeclareExchange(context.Background(), "x", "topic", ExchangeOptions{}) }()
	de := fc.expect(opDeclareExchange).(*declareExchangeHeader)
	if !de.Durable || de.Type != "topic" {
		t.Fatalf("declare_exchange = %+v", de)
	}
	// Reply out of order.
	fc.write(opError, &errorHeader{Rid: de.Rid, Code: 406, Msg: "PRECONDITION_FAILED"}, nil)
	fc.write(opOk, &okHeader{Rid: dq.Rid, Name: "amq.gen-1", Messages: 2, Consumers: 1}, nil)
	var se *ServerError
	if err := <-xerr; !errors.As(err, &se) || se.Code != 406 {
		t.Fatalf("declare_exchange err = %v", err)
	}
	if err := <-qerr; err != nil {
		t.Fatal(err)
	}
	if info != (QueueInfo{Name: "amq.gen-1", Messages: 2, Consumers: 1}) {
		t.Fatalf("info = %+v", info)
	}
}

func TestConfirms(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	ctx := context.Background()
	results := make([]chan error, 4)
	for i := range results {
		results[i] = make(chan error, 1)
		opts := PublishOptions{Mandatory: i == 1}
		go func(ch chan error) { ch <- c.PublishWithOptions(ctx, "ex", "k", []byte("x"), opts) }(results[i])
		p := fc.expect(opPublish).(*publishHeader)
		if p.Seq != uint64(i+1) || !p.Persistent {
			t.Fatalf("publish %d = %+v", i, p)
		}
	}
	// seq 2 is returned, seq 3 rejected, then everything up to 4 confirmed.
	fc.write(opReturn, &returnHeader{Seq: 2, Code: 312, Msg: "NO_ROUTE", Exchange: "ex", Key: "k"}, []byte("x"))
	fc.write(opConfirm, &confirmHeader{Seq: 3, Nack: true, Code: 406, Msg: "full"}, nil)
	fc.write(opConfirm, &confirmHeader{Upto: 4}, nil)
	if err := <-results[0]; err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	var ue *UnroutableError
	if err := <-results[1]; !errors.As(err, &ue) || ue.Exchange != "ex" || ue.RoutingKey != "k" || ErrorCode(err) != 312 {
		t.Fatalf("publish 2: %v", err)
	}
	var ne *NackedError
	if err := <-results[2]; !errors.As(err, &ne) || ne.Code != 406 {
		t.Fatalf("publish 3: %v", err)
	}
	if err := <-results[3]; err != nil {
		t.Fatalf("publish 4: %v", err)
	}

	// Without confirm, no sequence number.
	if err := c.PublishWithOptions(ctx, "", "q", nil, PublishOptions{NoConfirm: true, Transient: true}); err != nil {
		t.Fatal(err)
	}
	if p := fc.expect(opPublish).(*publishHeader); p.Seq != 0 || p.Persistent {
		t.Fatalf("publish = %+v", p)
	}
}

func TestPublishContextCancelled(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := c.Publish(ctx, "", "q", []byte("x")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	fc.expect(opPublish)
	// A late confirm is harmless and the next publish still works.
	fc.write(opConfirm, &confirmHeader{Upto: 1}, nil)
	done := make(chan error, 1)
	go func() { done <- c.Publish(context.Background(), "", "q", []byte("y")) }()
	if p := fc.expect(opPublish).(*publishHeader); p.Seq != 2 {
		t.Fatalf("seq = %d", p.Seq)
	}
	fc.write(opConfirm, &confirmHeader{Upto: 2}, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func subscribeFake(t *testing.T, c *Client, fc *fakeConn, opts SubscribeOptions) (*Subscription, *subscribeHeader) {
	t.Helper()
	type result struct {
		s   *Subscription
		err error
	}
	ch := make(chan result, 1)
	go func() {
		s, err := c.Subscribe(context.Background(), "jobs", opts)
		ch <- result{s, err}
	}()
	h := fc.expect(opSubscribe).(*subscribeHeader)
	fc.write(opOk, &okHeader{Rid: h.Rid}, nil)
	r := <-ch
	if r.err != nil {
		t.Fatal(r.err)
	}
	return r.s, h
}

func deliver(fc *fakeConn, sub uint32, from, to uint64) {
	for tag := from; tag <= to; tag++ {
		fc.write(opDeliver, &deliverHeader{Sub: sub, Tag: tag, Key: "jobs", Headers: map[string]any{"n": tag}}, []byte(fmt.Sprint(tag)))
	}
}

func next(t *testing.T, s *Subscription) *Message {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	m, err := s.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCreditTopUpsWithAcks(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	s, h := subscribeFake(t, c, fc, SubscribeOptions{Prefetch: 4})
	if h.Credit != 4 || h.NoAck || h.Queue != "jobs" {
		t.Fatalf("subscribe = %+v", h)
	}
	deliver(fc, h.Sub, 1, 4)
	msgs := make([]*Message, 4)
	for i := range msgs {
		msgs[i] = next(t, s)
		if msgs[i].DeliveryTag != uint64(i+1) || msgs[i].Headers["n"] != int64(i+1) {
			t.Fatalf("message %d = %+v", i, msgs[i])
		}
	}
	// Acking half the window grants it back in one credit frame.
	msgs[0].Ack()
	msgs[0].Ack() // settling twice is a no-op
	msgs[1].Nack(true)
	if a := fc.expect(opAck).(*ackHeader); a.Tag != 1 {
		t.Fatalf("ack = %+v", a)
	}
	if n := fc.expect(opNack).(*nackHeader); n.Tag != 2 || !n.Requeue {
		t.Fatalf("nack = %+v", n)
	}
	if cr := fc.expect(opCredit).(*creditHeader); cr.Sub != h.Sub || cr.Credit != 2 {
		t.Fatalf("credit = %+v", cr)
	}
	msgs[2].Ack()
	msgs[3].Ack()
	fc.expect(opAck)
	fc.expect(opAck)
	if cr := fc.expect(opCredit).(*creditHeader); cr.Credit != 2 {
		t.Fatalf("credit = %+v", cr)
	}
}

func TestCreditTopUpsWithNoAck(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	s, h := subscribeFake(t, c, fc, SubscribeOptions{NoAck: true})
	if h.Credit != DefaultPrefetch || !h.NoAck {
		t.Fatalf("subscribe = %+v", h)
	}
	deliver(fc, h.Sub, 1, 50)
	for i := 0; i < 50; i++ {
		m := next(t, s)
		if err := m.Ack(); err != nil { // no-op for NoAck messages
			t.Fatal(err)
		}
	}
	if cr := fc.expect(opCredit).(*creditHeader); cr.Credit != 50 {
		t.Fatalf("credit = %+v", cr)
	}
}

func TestCancelRequeuesBufferedMessages(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	s, h := subscribeFake(t, c, fc, SubscribeOptions{Prefetch: 10})
	deliver(fc, h.Sub, 1, 3)
	first := next(t, s) // the application holds tag 1
	time.Sleep(50 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- s.Cancel(context.Background()) }()
	u := fc.expect(opUnsubscribe).(*unsubscribeHeader)
	if u.Sub != h.Sub || u.Rid == 0 {
		t.Fatalf("unsubscribe = %+v", u)
	}
	requeued := map[uint64]bool{}
	for len(requeued) < 2 {
		n := fc.expect(opNack).(*nackHeader)
		if !n.Requeue {
			t.Fatalf("nack = %+v", n)
		}
		requeued[n.Tag] = true
	}
	if !requeued[2] || !requeued[3] {
		t.Fatalf("requeued %v, want tags 2 and 3", requeued)
	}
	// A delivery in flight before the broker saw the unsubscribe goes back too.
	deliver(fc, h.Sub, 4, 4)
	if n := fc.expect(opNack).(*nackHeader); n.Tag != 4 || !n.Requeue {
		t.Fatalf("nack = %+v", n)
	}
	fc.write(opOk, &okHeader{Rid: u.Rid}, nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := <-s.Messages(); ok {
		t.Fatal("Messages is still open")
	}
	if _, err := s.Next(context.Background()); !errors.Is(err, ErrCancelled) {
		t.Fatalf("Next err = %v", err)
	}
	// The held message can still be acknowledged.
	if err := first.Ack(); err != nil {
		t.Fatal(err)
	}
	if a := fc.expect(opAck).(*ackHeader); a.Tag != 1 {
		t.Fatalf("ack = %+v", a)
	}
}

func TestServerCancelDrainsBufferedMessages(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	s, h := subscribeFake(t, c, fc, SubscribeOptions{})
	deliver(fc, h.Sub, 1, 2)
	fc.write(opCancelled, &cancelledHeader{Sub: h.Sub, Code: 404, Msg: "queue 'jobs' was deleted"}, nil)
	var got []uint64
	for m := range s.Messages() {
		got = append(got, m.DeliveryTag)
	}
	if len(got) != 2 {
		t.Fatalf("got tags %v", got)
	}
	err := s.Err()
	var se *ServerError
	if !errors.Is(err, ErrCancelled) || !errors.As(err, &se) || se.Code != 404 {
		t.Fatalf("Err = %v", err)
	}
	if err := s.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServerCloseFailsPendingCalls(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	s, _ := subscribeFake(t, c, fc, SubscribeOptions{})
	errc := make(chan error, 2)
	go func() { _, err := c.PurgeQueue(context.Background(), "jobs"); errc <- err }()
	go func() { errc <- c.Publish(context.Background(), "", "jobs", nil) }()
	fc.next()
	fc.next()
	fc.write(opClose, &closeHeader{Code: 320, Msg: "CONNECTION_FORCED - shutdown"}, nil)
	if h := fc.expect(opClose).(*closeHeader); h.Code != 200 {
		t.Fatalf("close reply = %+v", h)
	}
	for i := 0; i < 2; i++ {
		if err := <-errc; !errors.Is(err, ErrClosed) || !strings.Contains(err.Error(), "320") {
			t.Fatalf("err = %v", err)
		}
	}
	waitDone(t, c)
	if _, ok := <-s.Messages(); ok {
		t.Fatal("Messages is still open")
	}
	if _, err := s.Next(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next err = %v", err)
	}
	if err := c.Publish(context.Background(), "", "q", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
}

func TestFatalErrorClosesTheConnection(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	fc.write(opError, &errorHeader{Code: 501, Msg: "FRAME_ERROR", Fatal: true}, nil)
	waitDone(t, c)
	err := c.Err()
	var se *ServerError
	if !errors.Is(err, ErrClosed) || !errors.As(err, &se) || se.Code != 501 {
		t.Fatalf("Err = %v", err)
	}
}

func TestPingIsAnswered(t *testing.T) {
	_, fc := connectFake(t, defaultWelcome)
	fc.write(opPing, nil, nil)
	fc.expect(opPong)
}

func TestHeartbeats(t *testing.T) {
	welcome := defaultWelcome
	welcome.Heartbeat = 1
	c, fc := connectFake(t, welcome)
	// The client pings when idle for half the interval.
	fc.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	f, err := readFrame(fc.r, defaultMaxFrame)
	if err != nil || f.op != opPing {
		t.Fatalf("got %+v, %v; want a ping", f, err)
	}
	// Silence for more than twice the interval closes the connection.
	start := time.Now()
	waitDone(t, c)
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("took %s to notice the dead server", elapsed)
	}
	if err := c.Err(); !errors.Is(err, ErrClosed) || !strings.Contains(err.Error(), "heartbeat") {
		t.Fatalf("Err = %v", err)
	}
}

func TestCloseHandshake(t *testing.T) {
	c, fc := connectFake(t, defaultWelcome)
	done := make(chan error, 1)
	go func() { done <- c.Close(context.Background()) }()
	if h := fc.expect(opClose).(*closeHeader); h.Code != 200 {
		t.Fatalf("close = %+v", h)
	}
	fc.write(opClose, &closeHeader{Code: 200}, nil)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return after the broker's close")
	}
	if !c.IsClosed() || !errors.Is(c.Err(), ErrClosed) {
		t.Fatalf("Err = %v", c.Err())
	}
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseStopsEveryGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	srv := newFakeServer(t, defaultWelcome)
	c, err := Connect(context.Background(), srv.url)
	if err != nil {
		t.Fatal(err)
	}
	fc := srv.accept()
	// Two subscriptions with undelivered messages nobody reads.
	for i := 0; i < 2; i++ {
		_, h := subscribeFake(t, c, fc, SubscribeOptions{})
		deliver(fc, h.Sub, uint64(i*10+1), uint64(i*10+3))
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// The broker never answers the close; Close gives up after closeWait.
		io.Copy(io.Discard, fc.conn)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := c.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fc.conn.Close()
	srv.ln.Close()
	wg.Wait()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("%d goroutines before, %d after Close:\n%s", before, runtime.NumGoroutine(), buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPublishTooLarge(t *testing.T) {
	welcome := defaultWelcome
	welcome.MaxFrame = maxHelloFrame
	c, _ := connectFake(t, welcome)
	err := c.Publish(context.Background(), "", "q", make([]byte, maxHelloFrame))
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v", err)
	}
}

func TestConnectTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			defer conn.Close()
			io.Copy(io.Discard, conn) // never answers hello
		}
	}()
	start := time.Now()
	_, err = ConnectWithOptions(context.Background(), "fire://"+ln.Addr().String(), ConnectOptions{ConnectTimeout: 200 * time.Millisecond})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("the timeout was not applied")
	}
}
