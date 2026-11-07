// Command driver runs the FireMQ SDK conformance suite against the Go SDK.
//
// It reads one JSON command per line on stdin and writes exactly one JSON reply per
// command on stdout, as described in spec/conformance.md in the FireMQ repository:
//
//	firemq-conformance run --driver "go run ./conformance/driver"
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	firemq "github.com/FireMQ/firemq-go"
)

// callTimeout bounds every SDK call so a stuck call fails instead of hanging the run.
const callTimeout = 30 * time.Second

type command struct {
	ID         json.RawMessage            `json:"id"`
	Op         string                     `json:"op"`
	URL        string                     `json:"url"`
	Name       string                     `json:"name"`
	Type       string                     `json:"type"`
	Durable    bool                       `json:"durable"`
	Exclusive  bool                       `json:"exclusive"`
	AutoDelete bool                       `json:"auto_delete"`
	Args       map[string]json.RawMessage `json:"args"`
	Queue      string                     `json:"queue"`
	Exchange   string                     `json:"exchange"`
	Key        string                     `json:"key"`
	Dest       string                     `json:"destination"`
	Source     string                     `json:"source"`
	Body       *string                    `json:"body"`
	BodyBase64 *string                    `json:"body_base64"`
	Persistent bool                       `json:"persistent"`
	Mandatory  bool                       `json:"mandatory"`
	Properties map[string]json.RawMessage `json:"properties"`
	Headers    map[string]json.RawMessage `json:"headers"`
	Prefetch   uint32                     `json:"prefetch"`
	NoAck      bool                       `json:"no_ack"`
	Sub        string                     `json:"sub"`
	TimeoutMS  int64                      `json:"timeout_ms"`
	Tag        string                     `json:"tag"`
	Requeue    bool                       `json:"requeue"`
}

type replyError struct {
	Kind    string  `json:"kind"`
	Code    *uint16 `json:"code,omitempty"`
	Message string  `json:"message"`
}

type reply struct {
	ID     json.RawMessage `json:"id"`
	OK     bool            `json:"ok"`
	Result any             `json:"result,omitempty"`
	Error  *replyError     `json:"error,omitempty"`
}

type driver struct {
	client   *firemq.Client
	subs     map[string]*firemq.Subscription
	messages map[string]*firemq.Message
	nextSub  int
}

func main() {
	d := &driver{subs: map[string]*firemq.Subscription{}, messages: map[string]*firemq.Message{}}
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 0, 1<<20), 256<<20)
	out := bufio.NewWriter(os.Stdout)
	for in.Scan() {
		line := bytes.TrimSpace(in.Bytes())
		if len(line) == 0 {
			continue
		}
		var cmd command
		var r reply
		if err := json.Unmarshal(line, &cmd); err != nil {
			r = reply{ID: json.RawMessage("null"), Error: &replyError{Kind: "other", Message: "invalid command: " + err.Error()}}
		} else {
			r = d.handle(&cmd)
			r.ID = cmd.ID
		}
		b, err := json.Marshal(r)
		if err != nil {
			b, _ = json.Marshal(reply{ID: r.ID, Error: &replyError{Kind: "other", Message: err.Error()}})
		}
		out.Write(b)
		out.WriteByte('\n')
		out.Flush()
	}
	if d.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		d.client.Close(ctx)
		cancel()
	}
}

func (d *driver) handle(cmd *command) reply {
	result, err := d.run(cmd)
	if err != nil {
		return reply{Error: classify(err)}
	}
	if result == nil {
		result = map[string]any{}
	}
	return reply{OK: true, Result: result}
}

func classify(err error) *replyError {
	e := &replyError{Kind: "other", Message: err.Error()}
	var se *firemq.ServerError
	var ue *firemq.UnroutableError
	var ne *firemq.NackedError
	switch {
	case errors.As(err, &ue):
		e.Kind = "unroutable"
		code := uint16(312)
		e.Code = &code
	case errors.As(err, &ne):
		e.Kind = "nacked"
		e.Code = &ne.Code
	case errors.Is(err, firemq.ErrClosed):
		e.Kind = "closed"
	case errors.As(err, &se):
		e.Kind = "server"
		e.Code = &se.Code
	case errors.Is(err, errUnsupported):
		e.Kind = "unsupported"
	}
	return e
}

var (
	errUnsupported = errors.New("unsupported op")
	errNotOpen     = fmt.Errorf("%w: not connected", firemq.ErrClosed)
)

func (d *driver) run(cmd *command) (any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if cmd.Op == "hello" {
		return map[string]any{"sdk": "firemq-go", "version": firemq.Version, "capabilities": []string{}}, nil
	}
	if cmd.Op == "connect" {
		if d.client != nil {
			d.client.Close(ctx)
		}
		d.reset()
		c, err := firemq.Connect(ctx, cmd.URL)
		if err != nil {
			return nil, err
		}
		d.client = c
		return nil, nil
	}
	c := d.client
	if c == nil {
		return nil, errNotOpen
	}
	switch cmd.Op {
	case "close":
		err := c.Close(ctx)
		d.client = nil
		d.reset()
		return nil, err
	case "declare_queue":
		args, err := table(cmd.Args)
		if err != nil {
			return nil, err
		}
		info, err := c.DeclareQueue(ctx, cmd.Name, firemq.QueueOptions{
			Transient:  !cmd.Durable,
			Exclusive:  cmd.Exclusive,
			AutoDelete: cmd.AutoDelete,
			Args:       args,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{"name": info.Name, "messages": info.Messages, "consumers": info.Consumers}, nil
	case "delete_queue":
		n, err := c.DeleteQueue(ctx, cmd.Name)
		if err != nil {
			return nil, err
		}
		return map[string]any{"messages": n}, nil
	case "purge_queue":
		n, err := c.PurgeQueue(ctx, cmd.Name)
		if err != nil {
			return nil, err
		}
		return map[string]any{"messages": n}, nil
	case "declare_exchange":
		return nil, c.DeclareExchange(ctx, cmd.Name, cmd.Type, firemq.ExchangeOptions{Transient: !cmd.Durable})
	case "delete_exchange":
		return nil, c.DeleteExchange(ctx, cmd.Name, false)
	case "bind_queue":
		args, err := table(cmd.Args)
		if err != nil {
			return nil, err
		}
		return nil, c.BindQueue(ctx, cmd.Queue, cmd.Exchange, cmd.Key, args)
	case "unbind_queue":
		return nil, c.UnbindQueue(ctx, cmd.Queue, cmd.Exchange, cmd.Key)
	case "bind_exchange":
		return nil, c.BindExchange(ctx, cmd.Dest, cmd.Source, cmd.Key)
	case "publish":
		return nil, d.publish(ctx, cmd)
	case "subscribe":
		sub, err := c.Subscribe(ctx, cmd.Queue, firemq.SubscribeOptions{Prefetch: cmd.Prefetch, NoAck: cmd.NoAck})
		if err != nil {
			return nil, err
		}
		d.nextSub++
		id := strconv.Itoa(d.nextSub)
		d.subs[id] = sub
		return map[string]any{"sub": id}, nil
	case "next":
		sub := d.subs[cmd.Sub]
		if sub == nil {
			return nil, fmt.Errorf("unknown subscription %q", cmd.Sub)
		}
		nctx, ncancel := context.WithTimeout(context.Background(), time.Duration(cmd.TimeoutMS)*time.Millisecond)
		defer ncancel()
		m, err := sub.Next(nctx)
		switch {
		case err == nil:
			return map[string]any{"message": d.message(m)}, nil
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, firemq.ErrCancelled):
			return map[string]any{"message": nil}, nil
		default:
			return nil, err
		}
	case "ack", "nack":
		m := d.messages[cmd.Tag]
		if m == nil {
			return nil, fmt.Errorf("unknown delivery tag %q", cmd.Tag)
		}
		delete(d.messages, cmd.Tag)
		if cmd.Op == "ack" {
			return nil, m.Ack()
		}
		return nil, m.Nack(cmd.Requeue)
	case "unsubscribe":
		sub := d.subs[cmd.Sub]
		if sub == nil {
			return nil, fmt.Errorf("unknown subscription %q", cmd.Sub)
		}
		return nil, sub.Cancel(ctx)
	case "get":
		m, err := c.Get(ctx, cmd.Queue, cmd.NoAck)
		if err != nil {
			return nil, err
		}
		if m == nil {
			return map[string]any{"message": nil}, nil
		}
		return map[string]any{"message": d.message(m)}, nil
	}
	return nil, fmt.Errorf("%w: %s", errUnsupported, cmd.Op)
}

func (d *driver) reset() {
	d.subs = map[string]*firemq.Subscription{}
	d.messages = map[string]*firemq.Message{}
}

func (d *driver) publish(ctx context.Context, cmd *command) error {
	var body []byte
	switch {
	case cmd.BodyBase64 != nil:
		b, err := base64.StdEncoding.DecodeString(*cmd.BodyBase64)
		if err != nil {
			return fmt.Errorf("invalid body_base64: %w", err)
		}
		body = b
	case cmd.Body != nil:
		body = []byte(*cmd.Body)
	}
	props, err := properties(cmd.Properties)
	if err != nil {
		return err
	}
	headers, err := table(cmd.Headers)
	if err != nil {
		return err
	}
	return d.client.PublishWithOptions(ctx, cmd.Exchange, cmd.Key, body, firemq.PublishOptions{
		Transient:  !cmd.Persistent,
		Mandatory:  cmd.Mandatory,
		Properties: props,
		Headers:    headers,
	})
}

// message converts a message to the suite's JSON form and remembers it for ack/nack.
func (d *driver) message(m *firemq.Message) map[string]any {
	tag := strconv.FormatUint(m.DeliveryTag, 10)
	d.messages[tag] = m
	var body any
	if utf8.Valid(m.Body) {
		body = string(m.Body)
	}
	headers := map[string]any{}
	for k, v := range m.Headers {
		headers[k] = jsonValue(v)
	}
	return map[string]any{
		"body":        body,
		"body_base64": base64.StdEncoding.EncodeToString(m.Body),
		"exchange":    m.Exchange,
		"routing_key": m.RoutingKey,
		"redelivered": m.Redelivered,
		"tag":         tag,
		"properties":  propertiesJSON(m.Properties),
		"headers":     headers,
	}
}

func properties(raw map[string]json.RawMessage) (firemq.Properties, error) {
	var p firemq.Properties
	for k, v := range raw {
		if string(v) == "null" {
			continue
		}
		var err error
		switch k {
		case "content_type":
			err = json.Unmarshal(v, &p.ContentType)
		case "content_encoding":
			err = json.Unmarshal(v, &p.ContentEncoding)
		case "correlation_id":
			err = json.Unmarshal(v, &p.CorrelationID)
		case "reply_to":
			err = json.Unmarshal(v, &p.ReplyTo)
		case "message_id":
			err = json.Unmarshal(v, &p.MessageID)
		case "type":
			err = json.Unmarshal(v, &p.Type)
		case "user_id":
			err = json.Unmarshal(v, &p.UserID)
		case "app_id":
			err = json.Unmarshal(v, &p.AppID)
		case "expiration":
			var ms uint64
			err = json.Unmarshal(v, &ms)
			p.Expiration = time.Duration(ms) * time.Millisecond
		case "timestamp":
			var secs int64
			err = json.Unmarshal(v, &secs)
			p.Timestamp = time.Unix(secs, 0).UTC()
		case "priority":
			err = json.Unmarshal(v, &p.Priority)
		}
		if err != nil {
			return p, fmt.Errorf("invalid property %s: %w", k, err)
		}
	}
	return p, nil
}

func propertiesJSON(p firemq.Properties) map[string]any {
	out := map[string]any{}
	set := func(k, v string) {
		if v != "" {
			out[k] = v
		}
	}
	set("content_type", p.ContentType)
	set("content_encoding", p.ContentEncoding)
	set("correlation_id", p.CorrelationID)
	set("reply_to", p.ReplyTo)
	set("message_id", p.MessageID)
	set("type", p.Type)
	set("user_id", p.UserID)
	set("app_id", p.AppID)
	if p.Expiration > 0 {
		out["expiration"] = p.Expiration.Milliseconds()
	}
	if !p.Timestamp.IsZero() {
		out["timestamp"] = p.Timestamp.Unix()
	}
	if p.Priority > 0 {
		out["priority"] = p.Priority
	}
	return out
}

// table converts JSON args or headers to SDK values: integers become int64 (or uint64
// when larger), other numbers float64.
func table(raw map[string]json.RawMessage) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		dec := json.NewDecoder(bytes.NewReader(v))
		dec.UseNumber()
		var val any
		if err := dec.Decode(&val); err != nil {
			return nil, fmt.Errorf("invalid value for %q: %w", k, err)
		}
		out[k] = fromJSON(val)
	}
	return out, nil
}

func fromJSON(v any) any {
	switch x := v.(type) {
	case json.Number:
		s := x.String()
		if !strings.ContainsAny(s, ".eE") {
			if i, err := strconv.ParseInt(s, 10, 64); err == nil {
				return i
			}
			if u, err := strconv.ParseUint(s, 10, 64); err == nil {
				return u
			}
		}
		f, _ := x.Float64()
		return f
	case []any:
		for i := range x {
			x[i] = fromJSON(x[i])
		}
		return x
	case map[string]any:
		for k := range x {
			x[k] = fromJSON(x[k])
		}
		return x
	}
	return v
}

// jsonValue converts a header value to JSON. Floats keep a fractional part so that
// 2.0 does not come back as the integer 2.
func jsonValue(v any) any {
	switch x := v.(type) {
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return nil
		}
		s := strconv.FormatFloat(x, 'g', -1, 64)
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		return json.Number(s)
	case []byte:
		return base64.StdEncoding.EncodeToString(x)
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = jsonValue(x[i])
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for k := range x {
			out[k] = jsonValue(x[k])
		}
		return out
	}
	return v
}
