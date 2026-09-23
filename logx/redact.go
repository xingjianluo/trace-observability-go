package logx

import (
	"fmt"
	"regexp"
	"strings"
)

// DefaultMask replaces sensitive values when no custom mask is set.
const DefaultMask = "***REDACTED***"

// Redactor rewrites log entries so configured sensitive values never
// reach a sink. Rules are applied to field keys (exact match, key set
// or case-insensitive suffix) and, optionally, to string values via
// regular expressions.
type Redactor struct {
	mask       string
	exactKeys  map[string]struct{}
	suffixes   []string
	patterns   []*regexp.Regexp
	patternSrc []string
}

// RedactRule is one redaction configuration entry.
type RedactRule struct {
	// Key matches a structured field name exactly (case-sensitive).
	Key string
	// Keys matches several field names exactly.
	Keys []string
	// KeySuffix matches field names ending with the suffix
	// (case-insensitive), e.g. "token", "password".
	KeySuffix string
	// KeySuffixes matches several suffixes.
	KeySuffixes []string
	// ValuePattern replaces every match of the Go regexp in any
	// string-valued field.
	ValuePattern string
}

// NewRedactor builds a Redactor from rules.
func NewRedactor(rules []RedactRule, mask string) (*Redactor, error) {
	if mask == "" {
		mask = DefaultMask
	}
	r := &Redactor{mask: mask, exactKeys: make(map[string]struct{})}
	for i, rule := range rules {
		if rule.Key != "" {
			r.exactKeys[rule.Key] = struct{}{}
		}
		for _, k := range rule.Keys {
			r.exactKeys[k] = struct{}{}
		}
		if rule.KeySuffix != "" {
			r.suffixes = append(r.suffixes, strings.ToLower(rule.KeySuffix))
		}
		for _, suf := range rule.KeySuffixes {
			r.suffixes = append(r.suffixes, strings.ToLower(suf))
		}
		if rule.ValuePattern != "" {
			re, err := regexp.Compile(rule.ValuePattern)
			if err != nil {
				return nil, fmt.Errorf("redact: rule %d has invalid value pattern %q: %w",
					i, rule.ValuePattern, err)
			}
			r.patterns = append(r.patterns, re)
			r.patternSrc = append(r.patternSrc, rule.ValuePattern)
		}
	}
	return r, nil
}

// Apply returns a copy of e with every matching field/pattern
// rewritten. The original entry and its maps/slices are never
// mutated.
func (r *Redactor) Apply(e Entry) Entry {
	out := e
	out.Fields = make([]Field, len(e.Fields))
	for i, f := range e.Fields {
		out.Fields[i] = r.redactField(f)
	}
	return out
}

func (r *Redactor) redactField(f Field) Field {
	if r.keyMatches(f.Key) {
		return Field{Key: f.Key, Value: r.mask}
	}
	return Field{Key: f.Key, Value: r.redactValue(f.Value)}
}

func (r *Redactor) keyMatches(key string) bool {
	if _, ok := r.exactKeys[key]; ok {
		return true
	}
	lk := strings.ToLower(key)
	for _, suf := range r.suffixes {
		if strings.HasSuffix(lk, suf) {
			return true
		}
	}
	return false
}

// redactValue walks strings, maps and slices so sensitive tokens
// nested inside payloads are replaced as well.
func (r *Redactor) redactValue(v any) any {
	switch x := v.(type) {
	case string:
		for _, re := range r.patterns {
			x = re.ReplaceAllString(x, r.mask)
		}
		return x
	case map[string]any:
		if len(x) == 0 {
			return x
		}
		cp := make(map[string]any, len(x))
		for k, vv := range x {
			if r.keyMatches(k) {
				cp[k] = r.mask
			} else {
				cp[k] = r.redactValue(vv)
			}
		}
		return cp
	case map[string]string:
		if len(x) == 0 {
			return x
		}
		cp := make(map[string]string, len(x))
		for k, vv := range x {
			if r.keyMatches(k) {
				cp[k] = r.mask
			} else {
				s := vv
				for _, re := range r.patterns {
					s = re.ReplaceAllString(s, r.mask)
				}
				cp[k] = s
			}
		}
		return cp
	case []any:
		cp := make([]any, len(x))
		for i, vv := range x {
			cp[i] = r.redactValue(vv)
		}
		return cp
	case []string:
		cp := make([]string, len(x))
		for i, vv := range x {
			s := vv
			for _, re := range r.patterns {
				s = re.ReplaceAllString(s, r.mask)
			}
			cp[i] = s
		}
		return cp
	default:
		return v
	}
}
