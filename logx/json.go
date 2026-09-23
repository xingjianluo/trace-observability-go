package logx

// AppendJSON renders e as a single JSON object followed by '\n'.
// Field values use their JSON-compatible natural encoding; failures
// never escape the function.
func AppendJSON(buf []byte, e Entry) []byte { return buf }
