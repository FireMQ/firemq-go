package firemq

import "testing"

func TestParseURL(t *testing.T) {
	cases := []struct {
		in   string
		want connectURL
	}{
		{"fire://localhost", connectURL{scheme: "fire", host: "localhost", port: 5673, vhost: "/", user: "guest", pass: "guest"}},
		{"fire://", connectURL{scheme: "fire", host: "localhost", port: 5673, vhost: "/", user: "guest", pass: "guest"}},
		{"fires://app:p%40ss@mq.example.com:7000/prod%2Feu", connectURL{scheme: "fires", host: "mq.example.com", port: 7000, vhost: "prod/eu", user: "app", pass: "p@ss"}},
		{"fires://mq.example.com", connectURL{scheme: "fires", host: "mq.example.com", port: 5674, vhost: "/", user: "guest", pass: "guest"}},
		{"fire://user@host/dev", connectURL{scheme: "fire", host: "host", port: 5673, vhost: "dev", user: "user", pass: "guest"}},
		{"fire://host/%2F", connectURL{scheme: "fire", host: "host", port: 5673, vhost: "/", user: "guest", pass: "guest"}},
		{"fire://host?vhost=qa", connectURL{scheme: "fire", host: "host", port: 5673, vhost: "qa", user: "guest", pass: "guest"}},
		{"fire://[::1]:5673", connectURL{scheme: "fire", host: "::1", port: 5673, vhost: "/", user: "guest", pass: "guest"}},
		{"ws://localhost:15672/fire?vhost=dev", connectURL{scheme: "ws", host: "localhost", port: 15672, vhost: "dev", user: "guest", pass: "guest", path: "/fire"}},
		{"wss://mq.example.com", connectURL{scheme: "wss", host: "mq.example.com", port: 15671, vhost: "/", user: "guest", pass: "guest", path: "/fire"}},
	}
	for _, tc := range cases {
		got, err := parseURL(tc.in)
		if err != nil {
			t.Errorf("parseURL(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseURL(%q) = %+v, want %+v", tc.in, got, tc.want)
		}
	}
}

func TestParseURLAuthority(t *testing.T) {
	u, err := parseURL("fire://[::1]:5673")
	if err != nil {
		t.Fatal(err)
	}
	if got := u.authority(); got != "[::1]:5673" {
		t.Fatalf("authority = %q", got)
	}
}

func TestParseURLRejectsOtherSchemes(t *testing.T) {
	for _, in := range []string{"amqp://localhost", "http://localhost", "localhost:5673"} {
		if _, err := parseURL(in); err == nil {
			t.Errorf("parseURL(%q): expected an error", in)
		}
	}
}
