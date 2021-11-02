// 该模块定义了爬虫相关的类型。
// @Author: Haart
// @Created: 2021-10-27
package crawler

type CrCode int

// 表示爬虫的返回值。
type ResponseWrapper struct {
	Code    string           `json:"code"`    // 响应码
	Message string           `json:"message"` // 响应消息。
	Items   []TrackingResult `json:"items"`   // 响应的运单集合。
}

// 表示爬虫返回值中的某个运单。
type TrackingResult struct {
	Code              CrCode          `json:"code"`              // 响应码。
	CodeMg            string          `json:"codeMg"`            // 响应码对应的消息。
	ReturnValue       string          `json:"returnValue"`       // 爬虫返回值。
	TrackingEventList []TrackingEvent `json:"trackingEventList"` // 爬虫抓取的所有事件集合。
	CMess             string          `json:"cMess"`             // 爬虫返回的消息。
	TrackingNo        string          `json:"trackingNo"`        // 对应的运单号。
}

// 表示爬虫返回值的某个运单的某个事件。
type TrackingEvent struct {
	Date    string `json:"date"`    // 日期。
	Place   string `json:"place"`   // 地点。
	Details string `json:"details"` // 事件明细。
}

const (
	CcSuccess     CrCode = 1   // 成功。
	CcSuccess2    CrCode = 200 // 成功。
	CcNoTracking  CrCode = 205 // 单号未查询到。
	CcParseFailed CrCode = 207 // 解析失败。
	CcOther       CrCode = 206 // 其它错误。
	CcTimeout     CrCode = 408 // 超时。
)

// 判断爬虫的返回码是否表示成功。
// 成功或者单号未查询到，都看作成功。
func IsSuccess(cc CrCode) bool {
	return cc == CcSuccess || cc == CcSuccess2 || cc == CcNoTracking
}
