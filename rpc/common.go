package rpc

const (
	rSuccess commonRspCode = "S" // 表示成功的查询。
	rError   commonRspCode = "E" // 表示出现错误的查询。
)

// 响应结果代码。
type commonRspCode string

type commonRsp struct {
	Status  commonRspCode       `json:"status"`  // 表示查询状态的代码，如果该字段是`rError`，那么`Data`字段不可用。
	Message string              `json:"message"` // 查询状态代码对应的文本。
	Data    []*trackingOrderRsp `json:"data"`    // 每个运单对应的查询结果。
}
