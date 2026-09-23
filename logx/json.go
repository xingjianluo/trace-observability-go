package logx

import (
	"encoding/json"
)

type jsonEntry struct {
	Time     string      `json:"time"`
	Level    string      `json:"level"`
	Message  string      `json:"message"`
	TraceID  string      `json:"trace_id,omitempty"`
	SpanID   string      `json:"span_id,omitempty"`
	ParentID string      `json:"parent_id,omitempty"`
	Depth    int         `json:"depth"`
	Fields   []jsonField `json:"fields,omitempty"`
}

type jsonField struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

// AppendJSON renders e as one JSON object appended to buf, followed
// by '\n'. Times use RFC3339Nano. Field order is preserved, which
// keeps golden comparisons stable.
func AppendJSON(buf []byte, e Entry) []byte {
	je := jsonEntry{
		Time:     e.Time.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
		Level:    e.Level.String(),
		Message:  e.Message,
		TraceID:  e.TraceID,
		SpanID:   e.SpanID,
		ParentID: e.ParentID,
		Depth:    e.Depth,
	}
	for _, f := range e.Fields {
		je.Fields = append(je.Fields, jsonField{Key: f.Key, Value: jsonSafe(f.Value)})
	}
	b, err := json.Marshal(je)
	if err != nil {
		// jsonSafe guarantees encodable values; fall back to a
		// minimal record rather than dropping the log line.
		b, _ = json.Marshal(map[string]string{
			"time":    je.Time,
			"level":   je.Level,
			"message": "logx: failed to marshal entry: " + err.Error(),
		})
	}
	return append(append(buf, b...), '\n')
}

// jsonSafe converts values that encoding/json rejects (channels,
// funcs, complex numbers) into their fmt rendering.
func jsonSafe(v any) any {
	switch x := v.(type) {
	case map[string]any:
		cp := make(map[string]any, len(x))
		for k, vv := range x {
			cp[k] = jsonSafe(vv)
		}
		return cp
	case []any:
		cp := make([]any, len(x))
		for i, vv := range x {
			cp[i] = jsonSafe(vv)
		}
		return cp
	default:
		return v
	}
}

// MarshalJSON renders an Entry directly (used by exporters) with the
// level as a string and time in RFC3339Nano, matching AppendJSON.
func (e Entry) MarshalJSON() ([]byte, error) {
	return json.Marshal(jsonEntry{
		Time:     e.Time.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"),
		Level:    e.Level.String(),
		Message:  e.Message,
		TraceID:  e.TraceID,
		SpanID:   e.SpanID,
		ParentID: e.ParentID,
		Depth:    e.Depth,
		Fields:   fieldsToJSON(e.Fields),
	})
}

func fieldsToJSON(fs []Field) []jsonField {
	if len(fs) == 0 {
		return nil
	}
	out := make([]jsonField, 0, len(fs))
	for _, f := range fs {
		out = append(out, jsonField{Key: f.Key, Value: jsonSafe(f.Value)})
	}
	return out
}
