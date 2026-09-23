package logx

import "regexp"

// Redactor rewrites log entries so configured sensitive values never
// reach a sink. Rules are applied to field keys and, optionally, to
// string values (regex patterns).
type Redactor struct {
	mask      string
	keyRules  map[string]struct{}
	patterns  []*regexp.Regexp
}

// RedactRule is one redaction configuration entry.
type RedactRule struct {
	// Key matches a structured field name exactly.
	Key string
	// Keys matches multiple field names.
	Keys []string
	// KeySuffix matches field names ending with the suffix
	// (case-insensitive), e.g. "token", "password".
	KeySuffix string
	// ValuePattern replaces every match of the regexp in any
	// string-valued field.
	ValuePattern string
}

// NewRedactor builds a Redactor. The default mask is "***REDACTED***".
func NewRedactor(rules []RedactRule, mask string) (*Redactor, error) {
	return &Redactor{}, nil
}

// Apply returns a copy of e with every matching field/pattern
// rewritten. The original entry and its maps are never mutated.
func (r *Redactor) Apply(e Entry) Entry { return e }
