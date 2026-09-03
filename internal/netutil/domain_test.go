package netutil

import "testing"

func TestExtractDomain(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		// Standard host:port
		{"www.google.co.uk:443", "google.co.uk"},
		{"api.sina.com.cn", "sina.com.cn"},
		{"example.com:8080", "example.com"},
		{"sub.example.com", "example.com"},

		// IP addresses
		{"192.168.1.1:8080", "192.168.1.1"},
		{"10.0.0.1", "10.0.0.1"},

		// IPv6
		{"[::1]:80", "::1"},
		{"[::1]", "::1"},

		// Localhost
		{"localhost", "localhost"},
		{"localhost:3000", "localhost"},

		// URLs
		{"https://www.google.co.uk/path", "google.co.uk"},
		{"http://api.example.com:8080/path?q=1", "example.com"},
		{"//example.com/path", "example.com"},

		// Bare domain
		{"example.com", "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ExtractDomain(tt.input)
			if got != tt.want {
				t.Errorf("ExtractDomain(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestExtractICANNDomain(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		// ICANN suffixes behave like eTLD+1.
		{"www.google.co.uk:443", "google.co.uk"},
		{"api.sina.com.cn", "sina.com.cn"},
		{"sub.example.com", "example.com"},
		{"example.com", "example.com"},

		// Private-section suffixes collapse to the provider's registrable domain.
		{"raw.githubusercontent.com", "githubusercontent.com"},
		{"camo.githubusercontent.com:443", "githubusercontent.com"},
		{"avatars.githubusercontent.com", "githubusercontent.com"},
		{"bucket.s3.amazonaws.com", "amazonaws.com"},
		{"myblog.blogspot.com", "blogspot.com"},

		// IP addresses, IPv6 and dotless names fall back to the host.
		{"192.168.1.1:8080", "192.168.1.1"},
		{"[::1]:80", "::1"},
		{"[::1]", "::1"},
		{"localhost:3000", "localhost"},

		// URLs and casing.
		{"https://RAW.GitHubUserContent.com/path", "githubusercontent.com"},
		{"http://api.example.com:8080/path?q=1", "example.com"},
		{"//Example.COM/path", "example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := ExtractICANNDomain(tt.input)
			if got != tt.want {
				t.Errorf("ExtractICANNDomain(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
