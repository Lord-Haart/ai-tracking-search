package utils

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	simpleDateTimeFormatAlt string = "2006-1-2 15:4:5"     // 表示 yyyy-M-d HH:mm:ss 格式的日期时间。
	simpleDateTimeFormat    string = "2006-01-02 15:04:05" //
)

// 根据配基准路径计算完整路径。
// base 基准路径。
// p 相对基准路径的路径。
func ToAbsPath(base, p string) string {
	if filepath.IsAbs(p) {
		return p
	}

	return filepath.Clean(filepath.Join(base, p))
}

func ParseInt(s string, dv int) int {
	if r, err := strconv.Atoi(s); err != nil {
		return dv
	} else {
		return r
	}
}

func ParseRFC1123(s string) time.Time {
	if r, err := time.Parse(time.RFC1123, s); err != nil {
		return time.UnixMilli(0)
	} else {
		return r
	}
}

// 使用若干格式尝试解析日期时间。
// s 待解析的字符串。
// 返回解析结果，具有本地时区。
func ParseTime(s string) time.Time {
	if r, err := time.ParseInLocation(simpleDateTimeFormatAlt, s, time.Local); err != nil {
		return time.Time{}
	} else {
		return r
	}
}

// 使用若干格式尝试解析日期时间。
// s 待解析的字符串。
// 返回解析结果，具有UTC时区。
func ParseUTCTime(s string) time.Time {
	if r, err := time.ParseInLocation(simpleDateTimeFormatAlt, s, time.UTC); err != nil {
		return time.Time{}
	} else {
		return r
	}
}

func FormatTime(t time.Time) string {
	return t.Format(simpleDateTimeFormat)
}

func AsInt(o interface{}, dv int) int {
	if r, ok := o.(int); ok {
		return r
	} else if r, ok := o.(string); ok {
		if r, err := strconv.Atoi(r); err != nil {
			return dv
		} else {
			return r
		}
	} else if r, ok := o.(bool); ok {
		if r {
			return 1
		} else {
			return 0
		}
	} else {
		return dv
	}
}

func AsString(o interface{}) string {
	if o == nil {
		return ""
	} else if r, ok := o.(string); ok {
		return r
	} else {
		return fmt.Sprintf("%T", o)
	}
}

func AsTime(o interface{}) time.Time {
	if r, ok := o.(time.Time); ok {
		return r
	} else if r, ok := o.(string); ok {
		return ParseTime(r)
	} else {
		return time.Time{}
	}
}

func AsBool(o interface{}) bool {
	if r, ok := o.(bool); ok {
		return r
	} else if r, ok := o.(int); ok {
		return r != 0
	} else if r, ok := o.(string); ok {
		s := strings.TrimSpace(strings.ToLower(r))
		return s == "true" || s == "on" || s == "yes"
	} else {
		return false
	}
}

// 反转字符串。
// s 原始字符串。
// 返回反转后的字符串，比如 `abc` 反转后的结果是 `cba`。
func ReverseString(s string) string {
	runes := []rune(s)
	for from, to := 0, len(runes)-1; from < to; from, to = from+1, to-1 {
		runes[from], runes[to] = runes[to], runes[from]
	}
	return string(runes)
}
