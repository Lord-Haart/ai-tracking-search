// 该模块定义了生成流水号的算法。
// @Author: Haart
// @Created: 2021-10-27
package utils

import (
	"fmt"
	"strconv"
	"time"
)

const (
	seqNoWorkId = 1
)

var (
	lastSeqNoTimestamp uint64
	lastSeqNoMiniSeq   uint64
)

func init() {
	lastSeqNoTimestamp = uint64(time.Now().UnixMilli())
	lastSeqNoMiniSeq = 0
}

// 生成一个新的流水号。
// 使用简化版的雪花算法。
func NewSeqNo() (string, error) {
	timestamp := uint64(time.Now().UnixMilli())

	// 获取当前时间戳如果小于上次时间戳，则表示时间戳获取出现异常
	if timestamp < lastSeqNoTimestamp {
		return "", fmt.Errorf("clock moved backwards. Refusing to generate id for %d milliseconds", lastSeqNoTimestamp-timestamp)
	}

	// 获取当前时间戳如果等于上次时间戳（同一毫秒内），则在序列号加一；否则序列号赋值为0，从0开始。
	if lastSeqNoTimestamp == timestamp {
		lastSeqNoMiniSeq = (lastSeqNoMiniSeq + 1) & ^(-1 << 12)
	} else {
		lastSeqNoMiniSeq = 0
	}

	// 将上次时间戳值刷新
	lastSeqNoTimestamp = timestamp

	return strconv.FormatUint((timestamp-1288834974657)<<(12+5+5)|(seqNoWorkId<<5)|lastSeqNoMiniSeq, 10), nil
}
