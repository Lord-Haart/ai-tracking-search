package utils

import (
	"net/http"
	"strings"
)

// 获取客户端的地址。
// request 表示客户端请求的Request对象。
// 返回客户端地址，可能包含各级代理服务器。
func GetRemoteAddr(request *http.Request) string {
	if hv := request.Header.Get("x-forwarded-for"); hv != "" {
		return hv
	} else {
		// 去掉端口号。
		ss := strings.SplitN(request.RemoteAddr, ":", 2)
		if len(ss) == 0 {
			return ""
		} else {
			return ss[0]
		}
	}
	// 	pp := strings.SplitN(hv, ",", 2)
	// 	if len(pp) == 0 {
	// 		result = request.RemoteAddr
	// 	} else {
	// 		result = strings.TrimSpace(pp[0])
	// 	}
	// } else {
	// 	result = request.RemoteAddr
	// }

	// pp := strings.SplitN(result, ":", 2)
	// if len(pp) == 0 {
	// 	return result
	// } else {
	// 	return pp[0]
	// }
}
