package banexg

import (
	"net/http"
	"strings"

	"go.uber.org/zap/zapcore"
)

type HttpHeader http.Header

func (h HttpHeader) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for k, v := range h {
		value := strings.Join(v, ",")
		if isSecretKey(k) {
			value = "[redacted]"
		}
		enc.AddString(k, value)
	}
	return nil
}
