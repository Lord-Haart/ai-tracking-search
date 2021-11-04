package rpc

import (
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"

	_db "com.cne/ai-tracking-search/db"
)

type carriersReq struct {
	idList []int64 `json:"id-list"`
}

type carriersRsp struct {
	commonRsp
	Data []*_db.CarrierPo `json:"data"`
}

// 执行运输商信息查询。
func Carriers(ctx *gin.Context) {
	req := carriersReq{}
	// now := time.Now()

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.AbortWithError(400, fmt.Errorf("illegal request: %w", err))
		return
	}

	rsp := carriersRsp{Data: _db.QueryAllCarrier()}
	rsp.Code = rSuccess
	rsp.Message = "success"
	rsp.ErrorId = 0

	ctx.JSON(http.StatusOK, rsp)
}
