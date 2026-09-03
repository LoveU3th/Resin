package netutil

import (
	"net"
	"net/url"
	"strings"

	"golang.org/x/net/publicsuffix"
)

// ExtractDomain extracts the effective top-level-domain-plus-one (eTLD+1)
// from a target string that may be host:port, a URL, an IPv6 address, etc.
//
// Examples:
//
//	"www.google.co.uk:443" -> "google.co.uk"
//	"api.sina.com.cn"      -> "sina.com.cn"
//	"192.168.1.1:8080"     -> "192.168.1.1"
//	"localhost"            -> "localhost"
//	"[::1]:80"             -> "::1"
func ExtractDomain(target string) string {
	host := extractHost(target)

	// Use the Public Suffix List to extract eTLD+1.
	// Returns error for IP addresses, localhost, or bare TLDs.
	if domain, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
		return domain
	}

	// Fallback: return host as-is (IP addresses, internal names, etc.).
	return host
}

// ExtractICANNDomain extracts the registrable domain using only the ICANN
// section of the Public Suffix List, ignoring the private section.
//
// The private section is where hosting/CDN providers register their shared
// domains (githubusercontent.com, cloudfront.net, netlify.app, ...). Those
// entries make eTLD+1 collapse to the full host, which is the right answer for
// cookie scoping but the wrong granularity when the goal is "one site, one
// egress IP". Ignoring the private section groups all hosts of one provider
// under the provider's own registrable domain.
//
// Examples:
//
//	"raw.githubusercontent.com"     -> "githubusercontent.com"  (private rule)
//	"camo.githubusercontent.com"    -> "githubusercontent.com"  (private rule)
//	"bucket.s3.amazonaws.com"       -> "amazonaws.com"          (private rule)
//	"www.google.co.uk"              -> "google.co.uk"           (ICANN rule)
//	"192.168.1.1:8080"              -> "192.168.1.1"
//	"localhost"                     -> "localhost"
func ExtractICANNDomain(target string) string {
	host := strings.ToLower(extractHost(target))
	if host == "" || net.ParseIP(host) != nil || !strings.Contains(host, ".") {
		return host
	}

	// Walk suffixes from longest to shortest and take the first one that is
	// itself an ICANN rule. Private-section rules are skipped, so hosts of one
	// provider unify under the provider's registrable domain.
	labels := strings.Split(host, ".")
	for i := 0; i < len(labels); i++ {
		candidate := strings.Join(labels[i:], ".")
		suffix, icann := publicsuffix.PublicSuffix(candidate)
		if !icann || suffix != candidate {
			continue
		}
		// Registrable domain: one label to the left of the ICANN suffix.
		if i == 0 {
			return candidate
		}
		return labels[i-1] + "." + candidate
	}

	return host
}

// extractHost strips scheme, port and IPv6 brackets from a target string.
func extractHost(target string) string {
	if strings.Contains(target, "://") || strings.HasPrefix(target, "//") {
		if u, err := url.Parse(target); err == nil && u.Host != "" {
			target = u.Host
		}
	}

	// net.SplitHostPort handles both "host:port" and "[ipv6]:port".
	if h, _, err := net.SplitHostPort(target); err == nil {
		return h
	}
	// Handle bare bracketed IPv6 like "[::1]" -> "::1".
	if strings.HasPrefix(target, "[") && strings.HasSuffix(target, "]") {
		return target[1 : len(target)-1]
	}
	return target
}
