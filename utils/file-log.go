package utils

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

type RollingFileLoggerWriter struct {
	date    time.Time // 日志的当前日期。
	Pattern string    // 日志使用的文件名模式。
	file    *os.File  // 当前的写入文件名。
}

func (fl *RollingFileLoggerWriter) Write(p []byte) (n int, err error) {
	today := time.Now().Truncate(24 * time.Hour)
	if today != fl.date {
		if fl.file != nil {
			fl.file.Sync()
			fl.file.Close()
		}
		fl.date = today
	}
	if fl.file == nil {
		fileName := fl.createFileName()
		dir := filepath.Dir(fileName)
		os.MkdirAll(dir, 0644)

		if f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
			return 0, err
		} else {
			fl.file = f
		}
	}

	return fl.file.Write(p)
}

func (fl *RollingFileLoggerWriter) createFileName() string {
	fn0 := strings.ReplaceAll(fl.Pattern, "$date", fl.date.Format("20060102"))
	if fn1, err := filepath.Abs(fn0); err != nil {
		return fn0
	} else {
		return fn1
	}
}
