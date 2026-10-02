package logging

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// RedactedPlaceholder 是整字段脱敏时写入的占位符。
const RedactedPlaceholder = "***REDACTED***"

// Action 定义对敏感字段采用的脱敏方式。
type Action int

const (
	// MaskFull 将整个值替换为固定占位符。
	MaskFull Action = iota
	// MaskPartial 保留首尾少量字符，中间打码，便于排障但不泄露全文。
	MaskPartial
	// MaskHash 使用带盐 SHA-256 摘要，输出稳定但不可逆的值。
	MaskHash
)

// Rule 描述一条字段脱敏规则。字段名匹配不区分大小写。
type Rule struct {
	Field  string
	Action Action
	Salt   string
}

// Masker 按字段名规则对结构化数据进行脱敏。
type Masker struct {
	full    map[string]struct{}
	partial map[string]struct{}
	hash    map[string]Rule
}

// NewMasker 依据规则创建 Masker。
func NewMasker(rules []Rule) *Masker {
	m := &Masker{
		full:    map[string]struct{}{},
		partial: map[string]struct{}{},
		hash:    map[string]Rule{},
	}
	for _, r := range rules {
		key := strings.ToLower(strings.TrimSpace(r.Field))
		if key == "" {
			continue
		}
		switch r.Action {
		case MaskPartial:
			m.partial[key] = struct{}{}
		case MaskHash:
			m.hash[key] = r
		default:
			m.full[key] = struct{}{}
		}
	}
	return m
}

// DefaultRules 覆盖常见敏感字段的内置规则集合（全部整字段打码，
// password 使用部分打码，user.id 使用带盐哈希）。
func DefaultRules() []Rule {
	return []Rule{
		{Field: "password", Action: MaskPartial},
		{Field: "passwd", Action: MaskFull},
		{Field: "token", Action: MaskFull},
		{Field: "access_token", Action: MaskFull},
		{Field: "refresh_token", Action: MaskFull},
		{Field: "authorization", Action: MaskFull},
		{Field: "secret", Action: MaskFull},
		{Field: "api_key", Action: MaskFull},
		{Field: "apikey", Action: MaskFull},
		{Field: "id_card", Action: MaskPartial},
		{Field: "phone", Action: MaskPartial},
		{Field: "email", Action: MaskPartial},
		{Field: "bank_card", Action: MaskPartial},
		{Field: "user.id", Action: MaskHash, Salt: "default-salt"},
	}
}

// MaskValue 对单个字符串值按指定动作脱敏。
func (m *Masker) MaskValue(v string, a Action, salt string) string {
	switch a {
	case MaskPartial:
		return partialMask(v)
	case MaskHash:
		return hashMask(v, salt)
	default:
		return RedactedPlaceholder
	}
}

// MaskFields 复制并脱敏 map 中的敏感字段，原始 map 不会被修改。
// 支持嵌套 map 与 map/slice 混合结构的递归脱敏。
func (m *Masker) MaskFields(fields map[string]any) map[string]any {
	if fields == nil {
		return nil
	}
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		out[k] = m.maskValue(k, v)
	}
	return out
}

// MaskStringMap 复制并脱敏 map[string]string（如组汇总属性）中的敏感字段，
// 原始 map 不会被修改；只有命中脱敏规则的字段才会被打码。
func (m *Masker) MaskStringMap(fields map[string]string) map[string]string {
	if fields == nil {
		return nil
	}
	out := make(map[string]string, len(fields))
	for k, v := range fields {
		out[k] = m.maskString(strings.ToLower(k), v)
	}
	return out
}

func (m *Masker) maskValue(field string, v any) any {
	lk := strings.ToLower(field)
	switch val := v.(type) {
	case map[string]any:
		nested := make(map[string]any, len(val))
		for k, vv := range val {
			nested[k] = m.maskValue(k, vv)
		}
		return nested
	case []any:
		out := make([]any, len(val))
		for i, vv := range val {
			out[i] = m.maskValue(field, vv)
		}
		return out
	case string:
		return m.maskString(lk, val)
	case nil:
		return nil
	default:
		// 非字符串值：命中规则时打码，未命中则原样保留（数值等）。
		if _, ok := m.full[lk]; ok {
			return RedactedPlaceholder
		}
		if _, ok := m.partial[lk]; ok {
			return RedactedPlaceholder
		}
		if _, ok := m.hash[lk]; ok {
			return RedactedPlaceholder
		}
		return v
	}
}

func (m *Masker) maskString(lk, v string) string {
	if _, ok := m.full[lk]; ok {
		return RedactedPlaceholder
	}
	if _, ok := m.partial[lk]; ok {
		return partialMask(v)
	}
	if r, ok := m.hash[lk]; ok {
		return hashMask(v, r.Salt)
	}
	return v
}

// partialMask 保留首 2、尾 1 个字符（长度不足 5 时整体打码）。
func partialMask(v string) string {
	r := []rune(v)
	n := len(r)
	switch {
	case n == 0:
		return ""
	case n < 5:
		return RedactedPlaceholder
	default:
		return string(r[:2]) + strings.Repeat("*", n-3) + string(r[n-1:])
	}
}

// hashMask 输出 "sha256:" + 带盐摘要前 16 个十六进制字符。
// 相同输入+盐永远得到相同输出，可用于关联但无法反推原值。
func hashMask(v, salt string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(salt))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(v))
	return "sha256:" + hex.EncodeToString(h.Sum(nil))[:16]
}
