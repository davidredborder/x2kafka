package flow

import (
	"fmt"
	"net"
	"strings"
)

// Default RFC 1918, RFC 6598, and RFC 4193 home subnets.
var defaultHomeSubnets = []string{
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"127.0.0.0/8",
	"100.64.0.0/10",
	"fc00::/7",
	"fe80::/10",
}

// HomeNetChecker classifies IP addresses as home/internal or external/WAN.
type HomeNetChecker struct {
	subnets []*net.IPNet
}

// NewHomeNetChecker creates a checker with the given subnets (or defaults if empty).
func NewHomeNetChecker(subnets []string) (*HomeNetChecker, error) {
	if len(subnets) == 0 {
		subnets = defaultHomeSubnets
	}

	checker := &HomeNetChecker{
		subnets: make([]*net.IPNet, 0, len(subnets)),
	}

	for _, s := range subnets {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if !strings.Contains(s, "/") {
			// Single IP: expand to /32 or /128
			ip := net.ParseIP(s)
			if ip == nil {
				return nil, fmt.Errorf("invalid IP in home_nets: %s", s)
			}
			if ip.To4() != nil {
				s += "/32"
			} else {
				s += "/128"
			}
		}

		_, ipNet, err := net.ParseCIDR(s)
		if err != nil {
			return nil, fmt.Errorf("invalid CIDR in home_nets %s: %w", s, err)
		}
		checker.subnets = append(checker.subnets, ipNet)
	}

	return checker, nil
}

// IsHome checks if the IP address belongs to any configured home network.
func (c *HomeNetChecker) IsHome(ipStr string) bool {
	if c == nil || len(c.subnets) == 0 || ipStr == "" {
		return false
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}
	for _, subnet := range c.subnets {
		if subnet.Contains(ip) {
			return true
		}
	}
	return false
}

// OrientFlow applies RedBorder Directional Model A:
// lan_ip is always the local home network IP, wan_ip is the external IP.
func (c *HomeNetChecker) OrientFlow(rec *JSONFlowRecord) {
	if c == nil || rec == nil || rec.LanIP == "" || rec.WanIP == "" {
		return
	}

	srcIsHome := c.IsHome(rec.LanIP)
	dstIsHome := c.IsHome(rec.WanIP)

	switch {
	case srcIsHome && !dstIsHome:
		rec.Direction = "upstream"
	case !srcIsHome && dstIsHome:
		rec.Direction = "downstream"
		// Swap client/server fields so LanIP is always the internal home_net IP
		rec.LanIP, rec.WanIP = rec.WanIP, rec.LanIP
		rec.LanL4Port, rec.WanL4Port = rec.WanL4Port, rec.LanL4Port
		rec.ClientMAC, rec.ServerMAC = rec.ServerMAC, rec.ClientMAC
		rec.LanInterface, rec.WanInterface = rec.WanInterface, rec.LanInterface
		rec.LanInterfaceName, rec.WanInterfaceName = rec.WanInterfaceName, rec.LanInterfaceName
		rec.LanInterfaceDescription, rec.WanInterfaceDescription = rec.WanInterfaceDescription, rec.LanInterfaceDescription
	case srcIsHome && dstIsHome:
		rec.Direction = "internal"
	default:
		if rec.Direction == "" {
			rec.Direction = "upstream"
		}
	}
}
