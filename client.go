package firemq

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Version is the SDK version sent to the broker in hello.
const Version = "0.1.0"

// closeWait bounds how long Close waits for the broker to acknowledge the close.
const closeWait = 2 * time.Second

var errClientClosed = closedBecause("client closed the connection")

// ServerInfo describes the broker, as announced in its welcome frame.
type ServerInfo struct {
	Name    string
	Version string
	// ConnID is the connection's name in the management API.
	ConnID string
	// Heartbeat is the negotiated heartbeat interval; zero when heartbeats are off.
	Heartbeat time.Duration
	// MaxFrame is the largest frame the broker accepts, in bytes.
	MaxFrame     uint32
	Capabilities []string
}

// QueueInfo is what DeclareQueue reports.
type QueueInfo struct {
	// Name is the queue's name; the broker picks one when DeclareQueue gets "".
	Name      string
	Messages  uint64
	Consumers uint32
}

// Client is a connection to a FireMQ broker. It is safe for concurrent use. Each
// Client runs one goroutine that reads from the socket and one that writes to it,
// plus one per active subscription; Close stops all of them.
type Client struct {
	conn      net.Conn
	server    ServerInfo
	maxFrame  uint32
	heartbeat time.Duration
	logger    *slog.Logger

	rid   atomic.Uint32
	subID atomic.Uint32

	// pubMu orders publishes so sequence numbers reach the broker in order.
	pubMu sync.Mutex
	seq   uint64

	mu       sync.Mutex
	requests map[uint32]chan reply
	confirms []pendingConfirm // sorted by seq
	returned map[uint64]*UnroutableError
	subs     map[uint32]*Subscription
	closeErr error // set once the connection is over
	closing  bool  // Close has sent close

	outMu sync.Mutex
	outQ  [][]byte
	wake  chan struct{}

	lastRecv atomic.Int64 // UnixNano of the last read from the socket
	done     chan struct{}
	doneOnce sync.Once
	wg       sync.WaitGroup
}

type reply struct {
	ok   *okHeader
	get  *getOkHeader
	body []byte
	err  error
}

type pendingConfirm struct {
	seq uint64
	ch  chan error
}

// Connect connects to a broker with a URL such as fire://user:pass@host:5673/vhost
// or fires://host for TLS. Missing parts default to user and password guest, vhost
// "/", port 5673 for fire:// and 5674 for fires://.
func Connect(ctx context.Context, url string) (*Client, error) {
	return ConnectWithOptions(ctx, url, ConnectOptions{})
}

// ConnectWithOptions is Connect with options for heartbeats, timeouts and TLS.
func ConnectWithOptions(ctx context.Context, url string, opts ConnectOptions) (*Client, error) {
	u, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	if u.scheme == "ws" || u.scheme == "wss" {
		return nil, fmt.Errorf("firemq: the Go SDK connects with fire:// or fires://, not %s://", u.scheme)
	}
	timeout := opts.ConnectTimeout
	if timeout <= 0 {
		timeout = DefaultConnectTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", u.authority())
	if err != nil {
		return nil, fmt.Errorf("firemq: connecting to %s: %w", u.authority(), err)
	}
	if u.tls() {
		cfg, err := tlsConfig(u.host, opts)
		if err != nil {
			conn.Close()
			return nil, err
		}
		tc := tls.Client(conn, cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			conn.Close()
			return nil, fmt.Errorf("firemq: TLS handshake with %s: %w", u.authority(), err)
		}
		conn = tc
	}

	c := &Client{
		conn:     conn,
		logger:   opts.Logger,
		requests: make(map[uint32]chan reply),
		returned: make(map[uint64]*UnroutableError),
		subs:     make(map[uint32]*Subscription),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	c.lastRecv.Store(time.Now().UnixNano())
	r := bufio.NewReaderSize(recvTracker{conn, &c.lastRecv}, 64*1024)

	// Abort blocking handshake I/O when ctx ends.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	welcome, err := handshake(conn, r, u, opts)
	if !stop() {
		if err == nil {
			err = ctx.Err()
		}
	}
	if err != nil {
		conn.Close()
		if ctx.Err() != nil && !isServerError(err) {
			return nil, fmt.Errorf("firemq: handshake with %s: %w", u.authority(), ctx.Err())
		}
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})

	c.maxFrame = welcome.MaxFrame
	if c.maxFrame == 0 {
		c.maxFrame = defaultMaxFrame
	}
	c.maxFrame = max(c.maxFrame, maxHelloFrame)
	c.heartbeat = time.Duration(welcome.Heartbeat) * time.Second
	c.server = ServerInfo{
		Name:         welcome.Server.Name,
		Version:      welcome.Server.Version,
		ConnID:       welcome.ConnID,
		Heartbeat:    c.heartbeat,
		MaxFrame:     c.maxFrame,
		Capabilities: welcome.Caps,
	}
	c.wg.Add(2)
	go c.readLoop(r)
	go c.writeLoop()
	return c, nil
}

func isServerError(err error) bool {
	var se *ServerError
	return errors.As(err, &se)
}

func tlsConfig(host string, opts ConnectOptions) (*tls.Config, error) {
	if opts.TLSConfig != nil {
		cfg := opts.TLSConfig.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = host
		}
		return cfg, nil
	}
	cfg := &tls.Config{ServerName: host, InsecureSkipVerify: opts.InsecureSkipVerify}
	if len(opts.CACertPEM) > 0 {
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(opts.CACertPEM) {
			return nil, errors.New("firemq: CACertPEM contains no valid certificate")
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// handshake sends FMQ1 and hello and reads the welcome.
func handshake(conn net.Conn, r *bufio.Reader, u connectURL, opts ConnectOptions) (*welcomeHeader, error) {
	heartbeat := opts.Heartbeat
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeat
	}
	var hbSecs uint32
	if heartbeat > 0 {
		hbSecs = uint32(max(heartbeat/time.Second, 1))
	}
	client := map[string]any{
		"name":    "firemq-go",
		"product": "firemq-go",
		"version": Version,
		"lang":    "go",
	}
	if opts.ConnectionName != "" {
		client["connection_name"] = opts.ConnectionName
	}
	hello, err := encodeFrame(opHello, &helloHeader{
		Ver:       protocolVersion,
		Vhost:     u.vhost,
		Auth:      authHeader{Mech: "plain", User: u.user, Pass: u.pass},
		Heartbeat: hbSecs,
		Client:    client,
		Caps:      []string{"confirms", "credit"},
	}, nil)
	if err != nil {
		return nil, err
	}
	if _, err := conn.Write(append(append([]byte{}, magic...), hello...)); err != nil {
		return nil, fmt.Errorf("firemq: sending hello: %w", err)
	}
	f, err := readFrame(r, maxHelloFrame)
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, closedBecause("server closed the connection during the handshake")
		}
		return nil, fmt.Errorf("firemq: reading welcome: %w", err)
	}
	h, err := decodeCommand(f)
	if err != nil {
		return nil, err
	}
	switch h := h.(type) {
	case *welcomeHeader:
		return h, nil
	case *errorHeader:
		return nil, &ServerError{Code: h.Code, Message: h.Msg}
	default:
		return nil, fmt.Errorf("firemq: expected welcome, got %s", opName(f.op))
	}
}

// recvTracker records when data last arrived, for heartbeat checks.
type recvTracker struct {
	net.Conn
	last *atomic.Int64
}

func (t recvTracker) Read(p []byte) (int, error) {
	n, err := t.Conn.Read(p)
	if n > 0 {
		t.last.Store(time.Now().UnixNano())
	}
	return n, err
}

// Server returns what the broker announced in the handshake.
func (c *Client) Server() ServerInfo { return c.server }

// Done returns a channel that is closed when the connection is over, whether Close was
// called or the connection failed.
func (c *Client) Done() <-chan struct{} { return c.done }

// IsClosed reports whether the connection is over.
func (c *Client) IsClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// Err returns why the connection ended, or nil while it is open. The error matches
// ErrClosed.
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// Close closes the connection gracefully: it tells the broker, waits up to two seconds
// or until ctx ends for the broker to acknowledge, then stops every goroutine the
// Client started. Unacknowledged messages go back to their queues. Pending calls fail
// with an error matching ErrClosed. Close is idempotent and returns nil.
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	initiate := c.closeErr == nil && !c.closing
	c.closing = true
	c.mu.Unlock()
	if initiate {
		if err := c.send(opClose, &closeHeader{Code: 200, Msg: "client closed"}, nil); err == nil {
			timer := time.NewTimer(closeWait)
			select {
			case <-c.done:
			case <-timer.C:
			case <-ctx.Done():
			}
			timer.Stop()
		}
		c.shutdown(errClientClosed)
	}
	c.wg.Wait()
	return nil
}

// shutdown ends the connection: it fails pending calls and confirms, ends
// subscriptions and stops the goroutines. Only the first reason is kept.
func (c *Client) shutdown(reason error) {
	if !errors.Is(reason, ErrClosed) {
		reason = &closedError{reason: reason}
	}
	c.mu.Lock()
	if c.closeErr != nil {
		c.mu.Unlock()
		return
	}
	c.closeErr = reason
	requests := c.requests
	c.requests = make(map[uint32]chan reply)
	confirms := c.confirms
	c.confirms = nil
	subs := c.subs
	c.subs = make(map[uint32]*Subscription)
	clear(c.returned)
	c.mu.Unlock()

	for _, ch := range requests {
		ch <- reply{err: reason}
	}
	for _, pc := range confirms {
		pc.ch <- reason
	}
	for _, s := range subs {
		s.end(reason)
	}
	// Let the writer flush what is queued, such as a close reply, but not forever.
	_ = c.conn.SetWriteDeadline(time.Now().Add(time.Second))
	c.doneOnce.Do(func() { close(c.done) })
}

// send encodes a frame and queues it for the writer.
func (c *Client) send(op byte, header any, payload []byte) error {
	b, err := encodeFrame(op, header, payload)
	if err != nil {
		return err
	}
	if err := c.checkSize(b); err != nil {
		return err
	}
	c.mu.Lock()
	err = c.closeErr
	c.mu.Unlock()
	if err != nil {
		return err
	}
	c.enqueue(b)
	return nil
}

func (c *Client) checkSize(b []byte) error {
	if size := len(b) - 4; size > int(c.maxFrame) {
		return fmt.Errorf("firemq: frame of %d bytes exceeds the broker's limit of %d bytes", size, c.maxFrame)
	}
	return nil
}

func (c *Client) enqueue(b []byte) {
	c.outMu.Lock()
	c.outQ = append(c.outQ, b)
	c.outMu.Unlock()
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// start registers a request and sends it. build receives the request id.
func (c *Client) start(op byte, build func(rid uint32) any) (uint32, chan reply, error) {
	rid := c.rid.Add(1)
	ch := make(chan reply, 1)
	c.mu.Lock()
	if c.closeErr != nil {
		err := c.closeErr
		c.mu.Unlock()
		return 0, nil, err
	}
	c.requests[rid] = ch
	c.mu.Unlock()
	if err := c.send(op, build(rid), nil); err != nil {
		c.mu.Lock()
		delete(c.requests, rid)
		c.mu.Unlock()
		return 0, nil, err
	}
	return rid, ch, nil
}

// await waits for the reply to a request.
func (c *Client) await(ctx context.Context, rid uint32, ch chan reply) (reply, error) {
	select {
	case r := <-ch:
		return r, r.err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.requests, rid)
		c.mu.Unlock()
		return reply{}, ctx.Err()
	}
}

func (c *Client) call(ctx context.Context, op byte, build func(rid uint32) any) (reply, error) {
	if err := ctx.Err(); err != nil {
		return reply{}, err
	}
	rid, ch, err := c.start(op, build)
	if err != nil {
		return reply{}, err
	}
	return c.await(ctx, rid, ch)
}

// DeclareExchange declares an exchange of kind "direct", "fanout", "topic" or
// "headers". The zero ExchangeOptions declares a durable exchange.
func (c *Client) DeclareExchange(ctx context.Context, name, kind string, opts ExchangeOptions) error {
	_, err := c.call(ctx, opDeclareExchange, func(rid uint32) any {
		return &declareExchangeHeader{
			Rid:        rid,
			Name:       name,
			Type:       kind,
			Durable:    !opts.Transient,
			AutoDelete: opts.AutoDelete,
			Internal:   opts.Internal,
			Passive:    opts.Passive,
			Args:       opts.Args,
		}
	})
	return err
}

// DeleteExchange deletes an exchange. With ifUnused it fails with code 406 when the
// exchange still has bindings.
func (c *Client) DeleteExchange(ctx context.Context, name string, ifUnused bool) error {
	_, err := c.call(ctx, opDeleteExchange, func(rid uint32) any {
		return &deleteExchangeHeader{Rid: rid, Name: name, IfUnused: ifUnused}
	})
	return err
}

// DeclareQueue declares a queue. The zero QueueOptions declares a durable queue; use
// TemporaryQueue for a private one. An empty name asks the broker to pick one, which
// is returned in QueueInfo.Name.
func (c *Client) DeclareQueue(ctx context.Context, name string, opts QueueOptions) (QueueInfo, error) {
	r, err := c.call(ctx, opDeclareQueue, func(rid uint32) any {
		return &declareQueueHeader{
			Rid:        rid,
			Name:       name,
			Durable:    !opts.Transient,
			Exclusive:  opts.Exclusive,
			AutoDelete: opts.AutoDelete,
			Passive:    opts.Passive,
			Args:       opts.Args,
		}
	})
	if err != nil {
		return QueueInfo{}, err
	}
	info := QueueInfo{Name: name}
	if r.ok != nil {
		if r.ok.Name != "" {
			info.Name = r.ok.Name
		}
		info.Messages = r.ok.Messages
		info.Consumers = r.ok.Consumers
	}
	return info, nil
}

// DeleteQueue deletes a queue and returns how many messages it held.
func (c *Client) DeleteQueue(ctx context.Context, name string) (uint64, error) {
	r, err := c.call(ctx, opDeleteQueue, func(rid uint32) any {
		return &deleteQueueHeader{Rid: rid, Name: name}
	})
	return okMessages(r), err
}

// PurgeQueue removes every ready message from a queue and returns how many it removed.
func (c *Client) PurgeQueue(ctx context.Context, name string) (uint64, error) {
	r, err := c.call(ctx, opPurgeQueue, func(rid uint32) any {
		return &purgeQueueHeader{Rid: rid, Name: name}
	})
	return okMessages(r), err
}

func okMessages(r reply) uint64 {
	if r.ok == nil {
		return 0
	}
	return r.ok.Messages
}

// BindQueue binds a queue to an exchange with a routing key or pattern. args may be
// nil; headers exchanges use it for "x-match" and the headers to match.
func (c *Client) BindQueue(ctx context.Context, queue, exchange, key string, args map[string]any) error {
	return c.bind(ctx, opBind, &bindHeader{Exchange: exchange, Queue: queue, Key: key, Args: args})
}

// UnbindQueue removes a binding made with BindQueue.
func (c *Client) UnbindQueue(ctx context.Context, queue, exchange, key string) error {
	return c.bind(ctx, opUnbind, &bindHeader{Exchange: exchange, Queue: queue, Key: key})
}

// BindExchange routes messages from exchange source to exchange destination.
func (c *Client) BindExchange(ctx context.Context, destination, source, key string) error {
	return c.bind(ctx, opBind, &bindHeader{Exchange: source, ToExchange: destination, Key: key})
}

// UnbindExchange removes a binding made with BindExchange.
func (c *Client) UnbindExchange(ctx context.Context, destination, source, key string) error {
	return c.bind(ctx, opUnbind, &bindHeader{Exchange: source, ToExchange: destination, Key: key})
}

func (c *Client) bind(ctx context.Context, op byte, h *bindHeader) error {
	_, err := c.call(ctx, op, func(rid uint32) any {
		h.Rid = rid
		return h
	})
	return err
}

// Publish publishes a persistent message and waits for the broker's confirm. Use
// exchange "" to send straight to the queue named by key.
func (c *Client) Publish(ctx context.Context, exchange, key string, body []byte) error {
	return c.PublishWithOptions(ctx, exchange, key, body, PublishOptions{})
}

// PublishJSON publishes v encoded as JSON with content type application/json and waits
// for the broker's confirm.
func (c *Client) PublishJSON(ctx context.Context, exchange, key string, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("firemq: encoding JSON body: %w", err)
	}
	return c.PublishWithOptions(ctx, exchange, key, body, PublishOptions{
		Properties: Properties{ContentType: "application/json"},
	})
}

// PublishWithOptions publishes a message. Unless opts.NoConfirm is set it waits for
// the broker's confirm and returns *NackedError when the broker rejects the message
// and *UnroutableError when a mandatory message reached no queue. The body is copied,
// so the caller may reuse it once PublishWithOptions returns.
func (c *Client) PublishWithOptions(ctx context.Context, exchange, key string, body []byte, opts PublishOptions) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	h := &publishHeader{
		Exchange:   exchange,
		Key:        key,
		Mandatory:  opts.Mandatory,
		Persistent: !opts.Transient,
		Props:      opts.Properties.toWire(),
		Headers:    opts.Headers,
	}
	if opts.NoConfirm {
		return c.send(opPublish, h, body)
	}

	ch := make(chan error, 1)
	c.pubMu.Lock()
	h.Seq = c.seq + 1
	b, err := encodeFrame(opPublish, h, body)
	if err == nil {
		err = c.checkSize(b)
	}
	if err == nil {
		c.mu.Lock()
		if err = c.closeErr; err == nil {
			c.confirms = append(c.confirms, pendingConfirm{seq: h.Seq, ch: ch})
		}
		c.mu.Unlock()
	}
	if err == nil {
		c.seq = h.Seq
		c.enqueue(b)
	}
	c.pubMu.Unlock()
	if err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe starts consuming from a queue. The broker sends up to opts.Prefetch
// messages (100 by default) before it needs acknowledgements; the SDK grants more
// credit in batches of half that window as messages are acknowledged, or as they are
// read when opts.NoAck is set.
func (c *Client) Subscribe(ctx context.Context, queue string, opts SubscribeOptions) (*Subscription, error) {
	window := opts.Prefetch
	if window == 0 {
		window = DefaultPrefetch
	}
	id := c.subID.Add(1)
	s := newSubscription(c, id, queue, window, opts.NoAck)
	c.mu.Lock()
	if c.closeErr != nil {
		err := c.closeErr
		c.mu.Unlock()
		return nil, err
	}
	c.subs[id] = s
	c.wg.Add(1)
	c.mu.Unlock()
	go s.run()

	_, err := c.call(ctx, opSubscribe, func(rid uint32) any {
		return &subscribeHeader{
			Rid:       rid,
			Sub:       id,
			Queue:     queue,
			Credit:    window,
			NoAck:     opts.NoAck,
			Exclusive: opts.Exclusive,
			Tag:       opts.Tag,
		}
	})
	if err != nil {
		c.removeSub(id, s)
		s.stopOnce.Do(func() { close(s.stop) })
		s.end(err)
		if ctx.Err() != nil {
			// The broker may have started the subscription after all.
			_ = c.send(opUnsubscribe, &unsubscribeHeader{Sub: id}, nil)
		}
		return nil, err
	}
	return s, nil
}

func (c *Client) removeSub(id uint32, s *Subscription) {
	c.mu.Lock()
	if c.subs[id] == s {
		delete(c.subs, id)
	}
	c.mu.Unlock()
}

// Get takes one message from a queue, or returns nil and no error when the queue is
// empty. Without noAck, acknowledge the message with Ack or Nack.
func (c *Client) Get(ctx context.Context, queue string, noAck bool) (*Message, error) {
	r, err := c.call(ctx, opGet, func(rid uint32) any {
		return &getHeader{Rid: rid, Queue: queue, NoAck: noAck}
	})
	if err != nil {
		return nil, err
	}
	if r.get == nil {
		return nil, nil
	}
	g := r.get
	m := &Message{
		Body:        r.body,
		Exchange:    g.Exchange,
		RoutingKey:  g.Key,
		Properties:  g.Props.toProperties(),
		Headers:     normalizeTable(g.Headers),
		Redelivered: g.Redelivered,
		DeliveryTag: g.Tag,
	}
	if !noAck {
		m.acker = &acker{client: c}
	}
	return m, nil
}

func (c *Client) warn(msg string, args ...any) {
	if c.logger != nil {
		c.logger.Warn(msg, args...)
	}
}

// writeLoop writes queued frames and sends heartbeats. It owns closing the socket.
func (c *Client) writeLoop() {
	defer c.wg.Done()
	defer c.conn.Close()
	w := bufio.NewWriterSize(c.conn, 64*1024)
	var tick <-chan time.Time
	if c.heartbeat > 0 {
		t := time.NewTicker(max(c.heartbeat/4, 100*time.Millisecond))
		defer t.Stop()
		tick = t.C
	}
	ping, _ := encodeFrame(opPing, nil, nil)
	lastSend := time.Now()
	for {
		var extra []byte
		select {
		case <-c.wake:
		case now := <-tick:
			if silence := now.Sub(time.Unix(0, c.lastRecv.Load())); silence > 2*c.heartbeat {
				c.shutdown(closedBecause("no data from the server for %s; heartbeats missed", silence.Round(time.Second)))
				continue
			}
			if now.Sub(lastSend) >= c.heartbeat/2 {
				extra = ping
			}
		case <-c.done:
			_, _ = c.flush(w, nil)
			return
		}
		wrote, err := c.flush(w, extra)
		if err != nil {
			c.shutdown(closedBecause("writing to the socket: %w", err))
			continue
		}
		if wrote {
			lastSend = time.Now()
		}
	}
}

// flush writes every queued frame, then extra. It reports whether it wrote anything.
func (c *Client) flush(w *bufio.Writer, extra []byte) (bool, error) {
	c.outMu.Lock()
	frames := c.outQ
	c.outQ = nil
	c.outMu.Unlock()
	if len(frames) == 0 && extra == nil {
		return false, nil
	}
	for _, f := range frames {
		if _, err := w.Write(f); err != nil {
			return true, err
		}
	}
	if extra != nil {
		if _, err := w.Write(extra); err != nil {
			return true, err
		}
	}
	return true, w.Flush()
}

// readLoop reads and dispatches frames until the connection ends.
func (c *Client) readLoop(r *bufio.Reader) {
	defer c.wg.Done()
	for {
		f, err := readFrame(r, c.maxFrame)
		if err != nil {
			switch {
			case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
				c.shutdown(closedBecause("server closed the connection"))
			case errors.Is(err, net.ErrClosed):
				c.shutdown(closedBecause("connection closed"))
			default:
				c.shutdown(closedBecause("reading from the socket: %w", err))
			}
			return
		}
		h, err := decodeCommand(f)
		if err != nil {
			c.warn("firemq: ignoring undecodable frame", "op", opName(f.op), "error", err)
			continue
		}
		if c.dispatch(f.op, h, f.payload) {
			return
		}
	}
}

// dispatch handles one frame from the broker. It returns true when the connection is
// over.
func (c *Client) dispatch(op byte, h any, payload []byte) bool {
	switch h := h.(type) {
	case *okHeader:
		c.resolve(h.Rid, reply{ok: h})
	case *getOkHeader:
		c.resolve(h.Rid, reply{get: h, body: payload})
	case *errorHeader:
		err := &ServerError{Code: h.Code, Message: h.Msg}
		if h.Rid != 0 {
			c.resolve(h.Rid, reply{err: err})
		} else if !h.Fatal {
			c.warn("firemq: broker reported an error", "code", h.Code, "msg", h.Msg)
		}
		if h.Fatal {
			c.shutdown(&closedError{reason: err})
			return true
		}
	case *confirmHeader:
		c.confirm(h)
	case *returnHeader:
		if h.Seq > 0 {
			c.mu.Lock()
			if c.closeErr == nil {
				c.returned[h.Seq] = &UnroutableError{Exchange: h.Exchange, RoutingKey: h.Key}
			}
			c.mu.Unlock()
		}
	case *deliverHeader:
		c.deliver(h, payload)
	case *cancelledHeader:
		c.mu.Lock()
		s := c.subs[h.Sub]
		delete(c.subs, h.Sub)
		c.mu.Unlock()
		if s != nil {
			s.cancelledByServer(fmt.Errorf("%w: %w", ErrCancelled, &ServerError{Code: h.Code, Message: h.Msg}))
		}
	case *closeHeader:
		c.mu.Lock()
		closing := c.closing
		c.mu.Unlock()
		if closing {
			c.shutdown(errClientClosed)
			return true
		}
		_ = c.send(opClose, &closeHeader{Code: 200}, nil)
		if h.Msg != "" {
			c.shutdown(closedBecause("closed by server (code %d): %s", h.Code, h.Msg))
		} else {
			c.shutdown(closedBecause("closed by server (code %d)", h.Code))
		}
		return true
	case nil:
		if op == opPing {
			_ = c.send(opPong, nil, nil)
		}
	}
	return false
}

// resolve hands a reply to the request waiting for it.
func (c *Client) resolve(rid uint32, r reply) {
	c.mu.Lock()
	ch := c.requests[rid]
	delete(c.requests, rid)
	c.mu.Unlock()
	if ch != nil {
		ch <- r
	}
}

// confirm settles publishes. A nack settles one sequence number; upto settles every
// outstanding one up to and including it, turning returned messages into
// *UnroutableError.
func (c *Client) confirm(h *confirmHeader) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if h.Nack {
		delete(c.returned, h.Seq)
		i := sort.Search(len(c.confirms), func(i int) bool { return c.confirms[i].seq >= h.Seq })
		if i < len(c.confirms) && c.confirms[i].seq == h.Seq {
			c.confirms[i].ch <- &NackedError{Code: h.Code, Message: h.Msg}
			c.confirms = append(c.confirms[:i], c.confirms[i+1:]...)
		}
		return
	}
	if h.Upto == 0 {
		return
	}
	n := 0
	for ; n < len(c.confirms) && c.confirms[n].seq <= h.Upto; n++ {
		pc := c.confirms[n]
		if ret, ok := c.returned[pc.seq]; ok {
			delete(c.returned, pc.seq)
			pc.ch <- ret
		} else {
			pc.ch <- nil
		}
	}
	c.confirms = c.confirms[n:]
	for seq := range c.returned {
		if seq <= h.Upto {
			delete(c.returned, seq)
		}
	}
}

// deliver queues a delivery on its subscription. Deliveries for a subscription that is
// being cancelled go back to the queue.
func (c *Client) deliver(h *deliverHeader, payload []byte) {
	c.mu.Lock()
	s := c.subs[h.Sub]
	c.mu.Unlock()
	if s == nil {
		return
	}
	m := &Message{
		Body:        payload,
		Exchange:    h.Exchange,
		RoutingKey:  h.Key,
		Properties:  h.Props.toProperties(),
		Headers:     normalizeTable(h.Headers),
		Redelivered: h.Redelivered,
		DeliveryTag: h.Tag,
	}
	if !s.noAck {
		m.acker = &acker{client: c, credit: s.credit}
	}
	if !s.push(m) && !s.noAck {
		_ = c.send(opNack, &nackHeader{Tag: h.Tag, Requeue: true}, nil)
	}
}
