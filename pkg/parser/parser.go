package parser

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Parser parses and extracts fields from raw log text messages.
type Parser struct {
	compiledRegexes []*regexp.Regexp
}

// NewDecoderParser creates a new Parser with optional pre-compiled regex patterns.
func NewDecoderParser(regexPatterns []string) (*Parser, error) {
	return NewParser(regexPatterns)
}

// NewParser creates a new Parser with optional pre-compiled regex patterns.
func NewParser(regexPatterns []string) (*Parser, error) {
	var compiled []*regexp.Regexp
	for _, pattern := range regexPatterns {
		if pattern == "" {
			continue
		}
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex pattern %q: %w", pattern, err)
		}
		compiled = append(compiled, re)
	}
	return &Parser{compiledRegexes: compiled}, nil
}

// ParseKV extracts key-value pairs from text (e.g., key=val key2="val two" key3='val three').
func ParseKV(text string) map[string]interface{} {
	fields := make(map[string]interface{})
	n := len(text)
	i := 0

	for i < n {
		// Skip whitespace
		for i < n && (text[i] == ' ' || text[i] == '\t' || text[i] == '\r' || text[i] == '\n') {
			i++
		}
		if i >= n {
			break
		}

		// Find key end (delimiter '=')
		keyStart := i
		for i < n && text[i] != '=' && text[i] != ' ' && text[i] != '\t' && text[i] != '\r' && text[i] != '\n' {
			i++
		}

		if i >= n || text[i] != '=' {
			// Not a key=value pair, skip word
			continue
		}

		key := text[keyStart:i]
		i++ // skip '='

		if key == "" {
			continue
		}

		// Parse value
		var valStr string
		if i < n && (text[i] == '"' || text[i] == '\'') {
			quote := text[i]
			i++ // skip opening quote
			valStart := i
			var sb strings.Builder
			escaped := false
			for i < n {
				ch := text[i]
				if escaped {
					sb.WriteByte(ch)
					escaped = false
				} else if ch == '\\' {
					escaped = true
				} else if ch == quote {
					break
				} else {
					sb.WriteByte(ch)
				}
				i++
			}
			if i < n && text[i] == quote {
				valStr = sb.String()
				i++ // skip closing quote
			} else {
				valStr = text[valStart:i]
			}
		} else {
			valStart := i
			for i < n && text[i] != ' ' && text[i] != '\t' && text[i] != '\r' && text[i] != '\n' {
				i++
			}
			valStr = text[valStart:i]
		}

		// Strip trailing commas/semicolons if present on unquoted values
		valStr = strings.TrimRight(valStr, ",;")

		// Parse interface value (integer vs string)
		if num, err := strconv.ParseInt(valStr, 10, 64); err == nil {
			fields[key] = num
		} else if numf, err := strconv.ParseFloat(valStr, 64); err == nil && strings.Contains(valStr, ".") {
			fields[key] = numf
		} else if boolVal, err := strconv.ParseBool(valStr); err == nil && (valStr == "true" || valStr == "false") {
			fields[key] = boolVal
		} else {
			fields[key] = valStr
		}
	}

	return fields
}

// ParseCEF extracts fields from standard Common Event Format strings.
// CEF Header: CEF:Version|Device Vendor|Device Product|Device Version|Signature ID|Name|Severity|Extension
func ParseCEF(text string) (map[string]interface{}, bool) {
	idx := strings.Index(text, "CEF:")
	if idx == -1 {
		return nil, false
	}

	cefText := text[idx:]
	parts := splitCEFHeader(cefText)
	if len(parts) < 7 {
		return nil, false
	}

	fields := make(map[string]interface{})
	fields["cef_version"] = parts[0]
	fields["cef_vendor"] = parts[1]
	fields["cef_product"] = parts[2]
	fields["cef_device_version"] = parts[3]
	fields["cef_signature_id"] = parts[4]
	fields["cef_name"] = parts[5]

	if sev, err := strconv.ParseInt(parts[6], 10, 64); err == nil {
		fields["cef_severity"] = sev
	} else {
		fields["cef_severity"] = parts[6]
	}

	if len(parts) >= 8 && parts[7] != "" {
		extKV := ParseKV(parts[7])
		for k, v := range extKV {
			fields[k] = v
		}
	}

	return fields, true
}

func splitCEFHeader(text string) []string {
	// Skip "CEF:" prefix
	text = strings.TrimPrefix(text, "CEF:")
	parts := make([]string, 0, 8)

	var sb strings.Builder
	escaped := false

	for i := 0; i < len(text); i++ {
		ch := text[i]
		if escaped {
			sb.WriteByte(ch)
			escaped = false
		} else if ch == '\\' {
			escaped = true
		} else if ch == '|' && len(parts) < 7 {
			parts = append(parts, sb.String())
			sb.Reset()
		} else {
			sb.WriteByte(ch)
		}
	}
	parts = append(parts, sb.String())
	return parts
}

// ParseRegex executes configured named capture group regexes against text.
func (p *Parser) ParseRegex(text string) map[string]interface{} {
	fields := make(map[string]interface{})
	for _, re := range p.compiledRegexes {
		match := re.FindStringSubmatch(text)
		if match == nil {
			continue
		}
		names := re.SubexpNames()
		for i, name := range names {
			if i > 0 && name != "" && match[i] != "" {
				valStr := match[i]
				if num, err := strconv.ParseInt(valStr, 10, 64); err == nil {
					fields[name] = num
				} else if numf, err := strconv.ParseFloat(valStr, 64); err == nil && strings.Contains(valStr, ".") {
					fields[name] = numf
				} else {
					fields[name] = valStr
				}
			}
		}
	}
	return fields
}

// FieldMappings defines standard redBorder canonical schema keys.
var FieldMappings = map[string]string{
	// Source IP
	"srcip":     "src_ip",
	"src":       "src_ip",
	"source_ip": "src_ip",
	"src_addr":  "src_ip",
	"client_ip": "src_ip",

	// Destination IP
	"dstip":          "dst_ip",
	"dst":            "dst_ip",
	"destination_ip": "dst_ip",
	"dst_addr":       "dst_ip",
	"target_ip":      "dst_ip",

	// Source Port
	"srcport":     "src_port",
	"spt":         "src_port",
	"source_port": "src_port",

	// Destination Port
	"dstport":          "dst_port",
	"dpt":              "dst_port",
	"destination_port": "dst_port",

	// Protocol / Transport
	"proto":    "transport",
	"protocol": "transport",

	// User
	"user":     "username",
	"usr":      "username",
	"suser":    "username",
	"duser":    "target_username",
	"dst_user": "target_username",

	// Action
	"act":    "action",
	"device": "hostname",
}

// NormalizeSchema converts extracted raw vendor keys to standardized redBorder schema keys.
func NormalizeSchema(fields map[string]interface{}) map[string]interface{} {
	normalized := make(map[string]interface{}, len(fields))
	for k, v := range fields {
		lowerK := strings.ToLower(k)
		if mappedKey, ok := FieldMappings[lowerK]; ok {
			normalized[mappedKey] = v
		} else {
			normalized[k] = v
		}
	}
	return normalized
}
