package rpc

import (
	"fmt"
	"log"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

const (
	rSuccess commonRspCode = "S" // 表示成功的查询。
	rError   commonRspCode = "E" // 表示出现错误的查询。
)

// 响应结果代码。
type commonRspCode string

type commonRsp struct {
	Status  commonRspCode `json:"status"`  // 表示查询状态的代码，如果该字段是`rError`，那么`Data`字段不可用。
	Message string        `json:"message"` // 查询状态代码对应的文本。
}

func recover500(ctx *gin.Context) {
	if err := recover(); err != nil {
		msg := fmt.Sprintf("%s\n%s", err, string(debug.Stack()))
		log.Printf("[ERROR] %s\n", msg)

		ctx.JSON(http.StatusOK, commonRsp{Status: rError, Message: msg})
	}
}
