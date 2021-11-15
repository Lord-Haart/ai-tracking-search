package utils

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type RollingFileLoggerWriter struct {
	date    time.Time  // 日志的当前日期。
	Pattern string     // 日志使用的文件名模式。
	file    *os.File   // 当前的写入文件名。
	lock    sync.Mutex // 同步锁。
}

func (fl *RollingFileLoggerWriter) Write(p []byte) (n int, err error) {
	today := time.Now().Truncate(24 * time.Hour)
	if today != fl.date {
		fl.closeFile()
		fl.date = today
	}

	if err := fl.ensureFileIsOpened(); err != nil {
		return 0, err
	}

	return fl.file.Write(p)
}

func (fl *RollingFileLoggerWriter) closeFile() {
	fl.lock.Lock()
	defer fl.lock.Unlock()

	if fl.file != nil {
		fl.file.Sync()
		fl.file.Close()
		fl.file = nil
	}
}

func (fl *RollingFileLoggerWriter) ensureFileIsOpened() error {
	fl.lock.Lock()
	defer fl.lock.Unlock()

	if fl.file == nil {
		fileName := fl.createFileName()
		dir := filepath.Dir(fileName)
		os.MkdirAll(dir, 0644)

		if f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err != nil {
			return err
		} else {
			fl.file = f
		}
	}

	return nil
}

func (fl *RollingFileLoggerWriter) createFileName() string {
	fn0 := strings.ReplaceAll(fl.Pattern, "$date", fl.date.Format("20060102"))
	if fn1, err := filepath.Abs(fn0); err != nil {
		return fn0
	} else {
		return fn1
	}
}
