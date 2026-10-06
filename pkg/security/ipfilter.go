package security

import (
	"net"
	"strings"
)

type IPFilter struct {
	allowedIPs  map[string]bool
	allowedNets []*net.IPNet
	blockedIPs  map[string]bool
	blockedNets []*net.IPNet
}

func NewIPFilter(allowed []string, blocked []string) *IPFilter {
	f := &IPFilter{
		allowedIPs: make(map[string]bool),
		blockedIPs: make(map[string]bool),
	}

	for _, item := range allowed {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			_, cidr, err := net.ParseCIDR(item)
			if err == nil && cidr != nil {
				f.allowedNets = append(f.allowedNets, cidr)
			}
		} else {
			if ip := net.ParseIP(item); ip != nil {
				f.allowedIPs[ip.String()] = true
			}
		}
	}

	for _, item := range blocked {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		if strings.Contains(item, "/") {
			_, cidr, err := net.ParseCIDR(item)
			if err == nil && cidr != nil {
				f.blockedNets = append(f.blockedNets, cidr)
			}
		} else {
			if ip := net.ParseIP(item); ip != nil {
				f.blockedIPs[ip.String()] = true
			}
		}
	}

	return f
}

// IsAllowed returns true if the remote IP address passes allowed and blacklist checks.
func (f *IPFilter) IsAllowed(ipStr string) bool {
	if ipStr == "" {
		return true
	}

	// Clean port if present (e.g. "192.168.1.1:51423" -> "192.168.1.1")
	if host, _, err := net.SplitHostPort(ipStr); err == nil {
		ipStr = host
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return true // Non-IP socket / path
	}

	cleanIP := ip.String()

	// 1. Check Blacklist / Blocked IPs
	if len(f.blockedIPs) > 0 || len(f.blockedNets) > 0 {
		if f.blockedIPs[cleanIP] {
			return false
		}
		for _, netBlock := range f.blockedNets {
			if netBlock.Contains(ip) {
				return false
			}
		}
	}

	// 2. Check Allowlist / Allowed IPs (if non-empty, IP MUST match)
	if len(f.allowedIPs) > 0 || len(f.allowedNets) > 0 {
		if f.allowedIPs[cleanIP] {
			return true
		}
		for _, netAllow := range f.allowedNets {
			if netAllow.Contains(ip) {
				return true
			}
		}
		return false
	}

	return true
}
