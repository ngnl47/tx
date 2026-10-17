package actor

import (
	"fmt"
	"strconv"
	"strings"
)

// Int64Key 编码有符号整数身份；与既有 i:十进制编码保持一致。
func Int64Key(value int64) string { return "i:" + strconv.FormatInt(value, 10) }

// Uint64Key 编码无符号整数身份；不同类型的相同值不会共用 Actor。
func Uint64Key(value uint64) string { return "u:" + strconv.FormatUint(value, 10) }

// StringKey 编码字符串身份；Identity.Key 也允许业务使用自定义编码。
func StringKey(value string) string { return "s:" + value }

// ParseInt64Key 解析标准有符号整数身份，不接受其他类型的键。
func ParseInt64Key(key string) (int64, error) {
	value, err := keyValue(key, "i:")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

// ParseUint64Key 解析标准无符号整数身份。
func ParseUint64Key(key string) (uint64, error) {
	value, err := keyValue(key, "u:")
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}

// ParseStringKey 解析标准字符串身份，保留字符串内的分隔符。
func ParseStringKey(key string) (string, error) { return keyValue(key, "s:") }
func keyValue(key, prefix string) (string, error) {
	value, ok := strings.CutPrefix(key, prefix)
	if !ok {
		return "", fmt.Errorf("actor: key %q requires prefix %q", key, prefix)
	}
	return value, nil
}
