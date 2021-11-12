// 该模块定义了生成流水号的算法。
// @Author: Haart
// @Created: 2021-10-27
package utils

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	workId uint64 = 1
)

var (
	lastSeqNoTimestamp uint64
	lastSeqNoMiniSeq   uint64

	localhostIP uint64
	lock        sync.Mutex
)

func init() {
	lastSeqNoTimestamp = uint64(time.Now().UnixMilli())
	lastSeqNoMiniSeq = 0

	if addrs, err := net.InterfaceAddrs(); err != nil {
		panic(err)
	} else {
		for _, addr := range addrs {
			if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
				if ipv4 := ip.IP.To4(); ipv4 != nil {
					localhostIP = uint64(uint32(ipv4[0])<<24 | uint32(ipv4[1])<<16 | uint32(ipv4[2])<<8 | uint32(ipv4[0]))
					break
				}
			}
		}
	}

	lock = sync.Mutex{}
}

// 生成一个新的流水号。
// 使用简化版的雪花算法。
func NewSeqNo() (string, error) {
	lock.Lock()
	defer lock.Unlock()

	timestamp := uint64(time.Now().UnixMilli())

	// 获取当前时间戳如果小于上次时间戳，则表示时间戳获取出现异常
	if timestamp < lastSeqNoTimestamp {
		return "", fmt.Errorf("clock moved backwards. Refusing to generate id for %d milliseconds", lastSeqNoTimestamp-timestamp)
	}

	// 获取当前时间戳如果等于上次时间戳（同一毫秒内），则在序列号加一；否则序列号赋值为0，从0开始。
	if lastSeqNoTimestamp == timestamp {
		lastSeqNoMiniSeq++
	} else {
		lastSeqNoMiniSeq = 0
	}

	// 将上次时间戳值刷新
	lastSeqNoTimestamp = timestamp

	return strconv.FormatUint((timestamp-1288834974657)<<(12+5+5)|(workId<<(12+5))|(localhostIP<<12)|(lastSeqNoMiniSeq & ^(-1<<12)), 10), nil
}
