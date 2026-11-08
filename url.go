package firemq

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// Default ports per URL scheme.
const (
	DefaultPort    = 5673
	DefaultTLSPort = 5674
)

// connectURL is a parsed fire://, fires://, ws:// or wss:// URL.
type connectURL struct {
	scheme string
	host   string
	port   int
	vhost  string
	user   string
	pass   string
	// path is the WebSocket endpoint, /fire by default. Empty for fire and fires.
	path string
}

func (u connectURL) tls() bool { return u.scheme == "fires" || u.scheme == "wss" }

// authority is host:port, with IPv6 hosts bracketed.
func (u connectURL) authority() string {
	return net.JoinHostPort(u.host, strconv.Itoa(u.port))
}

// parseURL parses a connection URL and fills in the defaults from the spec: user and
// password guest, vhost "/", and ports 5673 (fire), 5674 (fires), 15672 (ws) and
// 15671 (wss). For fire and fires the path is the vhost; for ws and wss it is the
// endpoint and the vhost is the "vhost" query parameter.
func parseURL(raw string) (connectURL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return connectURL{}, fmt.Errorf("firemq: invalid URL: %w", err)
	}
	var out connectURL
	out.scheme = strings.ToLower(u.Scheme)
	switch out.scheme {
	case "fire":
		out.port = DefaultPort
	case "fires":
		out.port = DefaultTLSPort
	case "ws":
		out.port = 15672
	case "wss":
		out.port = 15671
	default:
		return connectURL{}, fmt.Errorf("firemq: unsupported URL scheme %q; use fire or fires", u.Scheme)
	}
	out.host = u.Hostname()
	if out.host == "" {
		out.host = "localhost"
	}
	if p := u.Port(); p != "" {
		port, err := strconv.ParseUint(p, 10, 16)
		if err != nil {
			return connectURL{}, fmt.Errorf("firemq: invalid port %q", p)
		}
		out.port = int(port)
	}
	out.user, out.pass = "guest", "guest"
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			out.user = name
		}
		if pass, ok := u.User.Password(); ok {
			out.pass = pass
		}
	}
	queryVhost := "/"
	if v := u.Query().Get("vhost"); v != "" {
		queryVhost = v
	}
	switch out.scheme {
	case "fire", "fires":
		rawPath := strings.TrimLeft(u.EscapedPath(), "/")
		if rawPath == "" {
			out.vhost = queryVhost
		} else {
			vhost, err := url.PathUnescape(rawPath)
			if err != nil {
				return connectURL{}, fmt.Errorf("firemq: invalid vhost in URL path: %w", err)
			}
			out.vhost = vhost
		}
	default:
		out.path = u.EscapedPath()
		if out.path == "" || out.path == "/" {
			out.path = "/fire"
		}
		out.vhost = queryVhost
	}
	return out, nil
}
