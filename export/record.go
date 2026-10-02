package export

import "encoding/json"

// Kind 区分导出记录的类型。
type Kind int

const (
	KindSpan Kind = iota
	KindLog
	KindGroup
)

func (k Kind) String() string {
	switch k {
	case KindSpan:
		return "span"
	case KindLog:
		return "log"
	case KindGroup:
		return "group"
	default:
		return "unknown"
	}
}

// MarshalJSON 让 Kind 以字符串形式出现在输出中。
func (k Kind) MarshalJSON() ([]byte, error) {
	return json.Marshal(k.String())
}

// UnmarshalJSON 支持从字符串（或旧版数字）恢复 Kind。
func (k *Kind) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err == nil {
		switch name {
		case "span":
			*k = KindSpan
		case "log":
			*k = KindLog
		case "group":
			*k = KindGroup
		default:
			*k = Kind(-1)
		}
		return nil
	}
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return err
	}
	*k = Kind(n)
	return nil
}

// Record 是进入导出队列的统一记录。Data 使用 json.RawMessage，
// 以便在批次输出中原样内嵌 Span/日志的 JSON 对象，而不是 base64。
type Record struct {
	Kind     Kind            `json:"kind"`
	Data     json.RawMessage `json:"data"`
	TraceID  string          `json:"trace_id"`
	SpanID   string          `json:"span_id"`
	Accepted bool            `json:"accepted"`
}
