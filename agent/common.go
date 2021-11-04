// 该模块定义了查询代理相关的类型。
// @Author: Haart
// @Created: 2021-10-27
package agent

// 表示查询代理的返回码。
type AgCode int

// 表示查询代理的返回值。
type ResponseWrapper struct {
	Code    string           `json:"code"`    // 响应码
	Message string           `json:"message"` // 响应消息。
	Items   []TrackingResult `json:"items"`   // 响应的运单集合。
}

// 表示查询代理返回值中的某个运单。
type TrackingResult struct {
	Code              AgCode          `json:"code"`              // 响应码。
	CodeMg            string          `json:"codeMg"`            // 响应码对应的消息。
	ReturnValue       string          `json:"returnValue"`       // 查询代理返回值。
	TrackingEventList []TrackingEvent `json:"trackingEventList"` // 查询代理抓取的所有事件集合。
	CMess             string          `json:"cMess"`             // 查询代理返回的消息。
	TrackingNo        string          `json:"trackingNo"`        // 对应的运单号。
}

// 表示查询代理返回值的某个运单的某个事件。
type TrackingEvent struct {
	Date    string `json:"date"`    // 日期。
	Place   string `json:"place"`   // 地点。
	Details string `json:"details"` // 事件明细。
}

const (
	AcSuccess     AgCode = 1   // 成功。
	AcSuccess2    AgCode = 200 // 成功。
	AcNoTracking  AgCode = 205 // 单号未查询到。
	AcParseFailed AgCode = 207 // 解析失败。
	AcOther       AgCode = 206 // 其它错误。
	AcTimeout     AgCode = 408 // 超时。
)

// 判断查询代理的返回码是否表示成功。
// 成功或者单号未查询到，都看作成功。
func IsSuccess(cc AgCode) bool {
	return cc == AcSuccess || cc == AcSuccess2 || cc == AcNoTracking
}
