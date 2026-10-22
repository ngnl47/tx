package actor

import (
	"fmt"
	"strconv"
	"strings"
)

// Int64ID 编码有符号整数身份；与既有 i:十进制编码保持一致。
func Int64ID(value int64) string { return "i:" + strconv.FormatInt(value, 10) }

// Uint64ID 编码无符号整数身份；不同类型的相同值不会共用 Actor。
func Uint64ID(value uint64) string { return "u:" + strconv.FormatUint(value, 10) }

// StringID 编码字符串身份；Identity.ID 也允许业务使用自定义编码。
func StringID(value string) string { return "s:" + value }

// ParseInt64ID 解析标准有符号整数身份，不接受其他类型的身份编码。
func ParseInt64ID(id string) (int64, error) {
	value, err := idValue(id, "i:")
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(value, 10, 64)
}

// ParseUint64ID 解析标准无符号整数身份。
func ParseUint64ID(id string) (uint64, error) {
	value, err := idValue(id, "u:")
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(value, 10, 64)
}

// ParseStringID 解析标准字符串身份，保留字符串内的分隔符。
func ParseStringID(id string) (string, error) { return idValue(id, "s:") }
func idValue(id, prefix string) (string, error) {
	value, ok := strings.CutPrefix(id, prefix)
	if !ok {
		return "", fmt.Errorf("actor: id %q requires prefix %q", id, prefix)
	}
	return value, nil
}
