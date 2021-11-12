// 该模块定义了查询运单跟踪信息的外部接口。
// @Author: Haart
// @Created: 2021-10-27
package rpc

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	_agent "com.cne/ai-tracking-search/agent"
	_db "com.cne/ai-tracking-search/db"
	_rpcclient "com.cne/ai-tracking-search/rpcclient"
	_types "com.cne/ai-tracking-search/types"
	_utils "com.cne/ai-tracking-search/utils"

	"time"

	"github.com/gin-gonic/gin"
)

const (
	maxBatchSize int = 30 // 每个原始请求中允许包含的最多运单号。
)

// 表示查询请求。
type trackingsReq struct {
	CarrierCode string              `json:"carrierCode" binding:"required"` // 运输商代号。
	ClientId    string              `json:"clientId"`                       // 客户端ID。
	Timestamp   int64               `json:"timestamp"`                      // 时间戳。
	Language    _types.LangId       `json:"language"`                       // 期望返回的语言。
	Priority    _types.Priority     `json:"priority"`                       // 优先级(0-2)。
	Token       string              `json:"token"`                          // 和客户端ID对应的鉴权标记。
	Orders      []*trackingOrderReq `json:"orders"`                         // 请求包含的所有待查询运单。
}

// 表示查询请求中的一个运单。
type trackingOrderReq struct {
	TrackingNo string `json:"trackingNo"` // 运单号。
	Postcode   string `json:"postcode"`   // 收件人邮编。
	Dest       string `json:"dst"`        // 收件人地址。
	Date       string `json:"date"`       // 发件日期。
}

// 表示查询响应。
type trackingsRsp struct {
	commonRsp
	Data []*trackingOrderRsp `json:"data"` // 每个运单对应的查询结果。
}

// 表示查询响应中的一条运单。
type trackingOrderRsp struct {
	TrackingNo   string              `json:"trackingNo"`   // 运单号。
	SeqNo        string              `json:"seqNo"`        // 查询流水号。
	State        int                 `json:"state"`        // 查询状态，即查询代理是否返回了能够解析的查询结果（即使查询结果为空）。
	Message      string              `json:"message"`      // 运单状态对应的文本。
	Delivered    bool                `json:"delivered"`    // 是否已妥投。1表示已妥投，0表示未妥投。
	DeliveryDate string              `json:"deliveryDate"` // 妥投的时间。
	Destination  string              `json:"destination"`  // 妥投的目的地。
	Cached       bool                `json:"cached"`       // 此响应是否来自于缓存。
	CachedTime   string              `json:"cachedTime"`   // 此响应的缓存时间（UTC）。
	Events       []*trackingEventRsp `json:"events"`       // 运单包含的事件。
}

// 表示查询响应中的事件。
type trackingEventRsp struct {
	Date  string `json:"date"`  // 事件时间。
	State int    `json:"state"` // 事件状态。
	Place string `json:"place"` // 事件地点。
	Info  string `json:"info"`  // 事件详细。
}

// 执行运单跟踪状态查询。
func Trackings(ctx *gin.Context) {
	defer recover500(ctx)

	req := trackingsReq{Priority: _types.PriorityLow, Language: _types.LangEN}
	now := time.Now()

	if err := ctx.ShouldBindJSON(&req); err != nil {
		panic(fmt.Errorf("illegal request: %w", err))
	}

	validateReq(&req)

	// 为每个运单号构造一个查询对象。
	trackingSearchList := make([]*_rpcclient.TrackingSearch, 0, len(req.Orders))
	clientAddr := _utils.GetRemoteAddr(ctx.Request)
	for _, order := range req.Orders {
		if seqNo, err := _utils.NewSeqNo(); err != nil {
			// 无法获取新的流水号，处理下一个查询对象。
			continue
		} else {
			trackingSearchList = append(trackingSearchList, &_rpcclient.TrackingSearch{ReqTime: now, ClientAddr: clientAddr, SeqNo: seqNo, CarrierCode: req.CarrierCode, Language: req.Language, TrackingNo: order.TrackingNo, Done: false})
		}
	}

	// 从数据库中加载。
	loadTrackingResultFromDb(trackingSearchList)

	// t1 := time.Time{}
	// 未妥投的记录（包含数据库中查不到的记录），都需要通过查询代理爬取。
	// 将需要调用查询代理的记录推送到任务队列。
	var trackingSearchList2 []*_rpcclient.TrackingSearch = make([]*_rpcclient.TrackingSearch, 0)
	if keys, err := _rpcclient.PushTrackingSearchToQueue(_types.Priority(req.Priority), trackingSearchList); err != nil {
		// 推送查询对象到任务队列失败，放弃轮询缓存和拉取查询对象。
	} else {
		// 从缓存拉取查询对象（以及查询结果）。
		if trackingSearchList, err := _rpcclient.PullTrackingSearchFromCache(_types.Priority(req.Priority), keys); err != nil {
			panic(err)
		} else {
			// 匹配跟踪结果中的事件。
			matchAllEvents(trackingSearchList)

			// t1 = time.Now()
			// 来自查询代理的查询结果会被保存到数据库。
			// saveTrackingResultToDb(trackingSearchList) // 同步保存到数据库。

			go func() {
				defer _utils.RecoverPanic()

				saveTrackingResultToDb(trackingSearchList)
			}()

			trackingSearchList2 = append(trackingSearchList2, trackingSearchList...)
		}
	}

	ctx.JSON(http.StatusOK, buildTrackingsRsp(req.Orders, trackingSearchList, trackingSearchList2))

	// fmt.Printf("!!! %s\n", time.Now().Sub(t1))
}

// 验证请求参数是否合乎接口定义。
// req 待验证的请求参数。
func validateReq(req *trackingsReq) {
	now := time.Now()

	// 校验token。
	req.ClientId = strings.ToLower(strings.TrimSpace(req.ClientId))
	if req.ClientId != "" {
		clientTime := time.UnixMilli(req.Timestamp * 1000)
		if clientTime.Before(now.Add(-30 * time.Second)) {
			panic(fmt.Errorf("illegal timestamp"))
		}
		if clientSecret, err := loadClientSecret(req.ClientId); err != nil {
			panic(err)
		} else {
			plainText := fmt.Sprintf("%s%d%s", req.ClientId, req.Timestamp, clientSecret)
			md5Bytes := md5.Sum([]byte(plainText))
			token := hex.EncodeToString(md5Bytes[:])
			if token != req.Token {
				panic(fmt.Errorf("illegal token"))
			}
		}
	}

	// 校验运输商编号。
	req.CarrierCode = strings.ToLower(strings.TrimSpace(req.CarrierCode))
	if req.CarrierCode == "" {
		panic(fmt.Errorf("carrier code cannot be empty"))
	}

	// 校验运单号。
	pc := 0
	for _, order := range req.Orders {
		order.TrackingNo = strings.ToUpper(strings.TrimSpace(order.TrackingNo))
		if order.TrackingNo != "" {
			req.Orders[pc] = order
			pc++
		}
	}

	req.Orders = req.Orders[:pc]

	if len(req.Orders) == 0 {
		panic(fmt.Errorf("orders cannot be empty"))
	} else if len(req.Orders) > maxBatchSize {
		panic(fmt.Errorf("too many orders: [%d]", len(req.Orders)))
	}
}

func loadClientSecret(clientId string) (string, error) {
	if clientId == "cne" {
		return "jwVs72CJzNb7hOks#T@n9Z", nil
	} else {
		return "", fmt.Errorf("unknown client id: %s", clientId)
	}
}

// 从数据库中读取跟踪记录。
// trackingSearchList 待读取相应跟踪记录的查询对象。每个对象都要到数据库中查询一次。
func loadTrackingResultFromDb(trackingSearchList []*_rpcclient.TrackingSearch) {
	for i, ts := range trackingSearchList {
		// 跳过空单号，这种查询请求是不合法的。
		if ts.TrackingNo == "" {
			continue
		}

		tr := _db.QueryTrackingResultByTrackingNo(ts.CarrierCode, ts.Language, ts.TrackingNo)

		if tr != nil {
			ts.Src = _types.SrcDB
			ts.UpdateTime = tr.UpdateTime
			if tr.EventsJson != "" {
				json.Unmarshal([]byte(tr.EventsJson), &ts.Events)
			} else {
				ts.Events = []*_rpcclient.TrackingEvent{}
			}
			ts.AgentCode = _agent.AcSuccess2 // 如果跟踪记录来自于数据库，那么查询代理返回码字段固定为成功，因为该记录必然来自于之前曾经成功的查询。
			ts.Done = tr.Done
			trackingSearchList[i] = ts
		}
	}
}

func saveTrackingResultToDb(trackingSearchList []*_rpcclient.TrackingSearch) {
	now := time.Now()
	for _, ts := range trackingSearchList {
		var eventsJson string
		if len(ts.Events) == 0 {
			eventsJson = ""
		} else {
			if eventsJsonBytes, err := json.Marshal(ts.Events); err != nil {
				panic(err)
			} else {
				eventsJson = string(eventsJsonBytes)
			}
		}

		carrierPo := _db.QueryCarrierByCode(ts.CarrierCode)
		if carrierPo != nil {
			if _db.SaveTrackingResultToDb(carrierPo.Id, ts.Language, ts.TrackingNo, eventsJson, now, ts.Done) <= 0 {
				log.Printf("[INFO] Duplicated tracking result(carrier-code=%s, language=%s, tracking-no=%s\n", ts.CarrierCode, ts.Language.String(), ts.TrackingNo)
				continue
			}

			trackingId := _db.SaveTrackingToDb(carrierPo.Id, ts.Language, ts.TrackingNo, ts.DoneTime, ts.DonePlace, ts.Src, ts.AgentName, now, ts.Done)
			for _, event := range ts.Events {
				_db.SaveTrackingDetailToDb(trackingId, event.Date, event.Place, event.Details, event.State, now)
			}
		}
	}
}

func saveLogToDb(trackingSearchList []*_rpcclient.TrackingSearch) {
	now := time.Now()
	operator := "auto"
	for _, ts := range trackingSearchList {
		matchType := 2 // 外部接口指定carrierCode。
		resultStatus := 0
		resultNote := ""
		if ts.AgentCode == _agent.AcSuccess || ts.AgentCode == _agent.AcSuccess2 {
			resultStatus = 1
			if ts.Src != _types.SrcDB {
				resultNote = "查询成功"
			} else if ts.Done {
				resultNote = "查询缓存成功（已妥投）"
			} else {
				resultNote = "查询缓存成功（未妥投）"
			}
		} else if ts.AgentCode == _agent.AcNoTracking {
			resultStatus = 1
			resultNote = "未查询到单号"
		} else if ts.AgentCode == _agent.AcParseFailed {
			resultNote = "无法解析目标网站页面"
		} else if ts.AgentCode == _agent.AcTimeout {
			resultNote = "查询目标网站超时"
		} else {
			resultNote = "未知错误"
		}

		if resultStatus == 0 && ts.Err != "" {
			resultNote = resultNote + ": " + ts.Err
		}

		var timing int64
		var endTime time.Time
		if !_utils.IsZeroTime(ts.AgentEndTime) {
			endTime = ts.AgentEndTime
		} else {
			endTime = time.Now()
		}

		timing = endTime.Sub(ts.ReqTime).Milliseconds()

		carrierPo := _db.QueryCarrierByCode(ts.CarrierCode)
		carrierId := int64(0)
		countryId := 0
		if carrierPo != nil {
			// 没有匹配到运输商。但是也应该记录日志。
			carrierId = carrierPo.Id
			countryId = carrierPo.CountryId
		}
		_db.SaveTrackingLogToDb(carrierId, ts.TrackingNo, matchType, countryId, int(timing), ts.ClientAddr, resultStatus, now, ts.Src, now, operator,
			ts.ReqTime, ts.AgentStartTime, ts.AgentEndTime, ts.AgentRawText, resultNote)
	}
}

// 匹配查询对象集合中包含的事件。
// trackingSearchList 待匹配的查询对象集合。
func matchAllEvents(trackingSearchList []*_rpcclient.TrackingSearch) {
	for i, ts := range trackingSearchList {
		rules := _db.QueryMatchRuleByCarrierCode(ts.CarrierCode, ts.ReqTime)

		matchEvents(rules, ts)
		trackingSearchList[i] = ts
	}
}

// 匹配查询对象中的事件。
// rules 关联的匹配规则。
// 待匹配的查询对象。
func matchEvents(rules []*_db.MatchRulePo, ts *_rpcclient.TrackingSearch) {
	// 按事件排序。
	sort.Stable(ts.Events)

	for i, evt := range ts.Events {
		// 两次遍历，第一次针对查询代理类别的规则进行匹配。
		matched := false
		delivered := false
		evt.State = 2
		for _, rule := range rules {
			if rule.TargetType != "4" && rule.Match(evt.Details) {
				evt.State, delivered = matchRuleCodeToState(rule.Code)
				matched = true
				break
			}
		}

		if !matched {
			// 第二次针对运输商类别的规则进行匹配。
			for _, rule := range rules {
				if rule.TargetType == "4" && rule.Match(evt.Details) {
					evt.State, delivered = matchRuleCodeToState(rule.Code)
					break
				}
			}
		}
		ts.Events[i] = evt

		// 如果某个事件匹配到了已妥投，那么设置整个查询对象的状态为已妥投，并设置妥投时间和妥投地点。
		if delivered {
			ts.Done = true
			ts.DoneTime = evt.Date
			ts.DonePlace = evt.Place
		}
	}
}

// 将匹配规则代码映射为响应结果中的状态码。
// code 匹配规则代码。
// 返回对应的响应结果状态码，以及是否映射成功。
func matchRuleCodeToState(code string) (int, bool) {
	if code == "Delivered" {
		return 3, true // 表示已妥投。
	} else if code == "Undelivered" {
		return 8, false // 表示投递失败。
	} else {
		return 2, false // 表示状态未知。
	}
}

// 构造最终的响应结果。
// orders 待查询的运单。
// r1 来自数据库的响应结果。
// r2 来自查询代理的响应结果。
// 返回组合后的结果。返回结果按照 `orders` 的顺序排序。首先从r2中获取结果，如果不存在则尝试从r1中返回结果，否则返回一个表示未查询到的结果。
func buildTrackingsRsp(orders []*trackingOrderReq, r1, r2 []*_rpcclient.TrackingSearch) *trackingsRsp {
	data := make([]*trackingOrderRsp, 0, len(orders))

	isOk_ := func(ts0 *_rpcclient.TrackingSearch) bool {
		return ts0 != nil && (ts0.Err == "" || ts0.Err == "success")
	}

	logList := make([]*_rpcclient.TrackingSearch, 0)
	for _, orderReq := range orders {
		var ts_ *_rpcclient.TrackingSearch
		ts2, ok2 := findTrackingSearchByTrackingNo(r2, orderReq.TrackingNo)
		ts1, ok1 := findTrackingSearchByTrackingNo(r1, orderReq.TrackingNo)

		if ok2 && !ok1 {
			ts_ = ts2
		} else if !ok2 && ok1 {
			ts_ = ts1
		} else if !ok2 && !ok1 {
			data = append(data, buildEmptyTrackingOrderResult(orderReq.TrackingNo))
			continue
		} else {
			if isOk_(ts1) && !isOk_(ts2) {
				ts_ = ts1
			} else {
				ts_ = ts2
			}
		}
		data = append(data, buildTrackingOrderResult(ts_))
		logList = append(logList, ts_)
	}

	// saveLogToDb(logList) // 同步保存到数据库。

	go func() {
		defer _utils.RecoverPanic()

		saveLogToDb(logList)
	}()

	result := trackingsRsp{Data: data}
	result.Status = rSuccess
	result.Message = "success"

	return &result
}

// 从查询对象集合中寻找运单号匹配的的查询对象。
// trackingSearchList 查询对象集合。
// trackingNo 待查询的运单号。
func findTrackingSearchByTrackingNo(trackingSearchList []*_rpcclient.TrackingSearch, trackingNo string) (*_rpcclient.TrackingSearch, bool) {
	for _, ts := range trackingSearchList {
		if ts.TrackingNo == trackingNo {
			return ts, true
		}
	}

	return nil, false
}

// 根据运单查询对象构造一个运单查询响应对象。
// trackingSearch 运单查询对象。
// 返回已构造的运单查询响应对象。
func buildTrackingOrderResult(trackingSearch *_rpcclient.TrackingSearch) *trackingOrderRsp {
	events := make([]*trackingEventRsp, 0, len(trackingSearch.Events))
	for _, evt := range trackingSearch.Events {
		events = append(events, &trackingEventRsp{
			Date:  _utils.FormatTime(evt.Date), // TODO: UTC?
			State: evt.State,
			Place: evt.Place,
			Info:  evt.Details,
		})
	}

	cached := trackingSearch.Src == _types.SrcDB
	cachedTime := ""
	if cached {
		cachedTime = _utils.FormatTime(trackingSearch.UpdateTime.In(time.UTC))
	}

	result := trackingOrderRsp{
		TrackingNo:   trackingSearch.TrackingNo,
		SeqNo:        trackingSearch.SeqNo,
		Message:      "",
		Delivered:    false,
		DeliveryDate: "",
		Destination:  "",
		Cached:       cached,
		CachedTime:   cachedTime,
		Events:       events,
	}

	if _agent.IsSuccess(trackingSearch.AgentCode) {
		result.State = 1 // 表示此结果来自于数据库或者查询代理爬取的有效网页内容。
		if trackingSearch.Done {
			result.Delivered = true
			result.DeliveryDate = _utils.FormatTime(trackingSearch.DoneTime) // TODO: UTC?
			result.Destination = trackingSearch.DonePlace
		}
	} else {
		result.State = 0 // 表示查询代理发去的网页内容无效（不存在或者无法解析）。
		result.Message = trackingSearch.Err
	}

	return &result
}

// 构造一个空的表示无效的跟踪记录。
// TMS 调用端要求返回结果中的运单记录和请求中的运单记录数量、顺序保持一致。
// 如果既不能从数据库，也不能从查询代理获取跟踪结果，那么调用此方法生成一个。
// trackingNo 运单号。
func buildEmptyTrackingOrderResult(trackingNo string) *trackingOrderRsp {
	return &trackingOrderRsp{
		TrackingNo:   trackingNo,
		SeqNo:        "",
		State:        0, // 表示此结果是凭空构造的。
		Message:      "Timeout",
		Delivered:    false,
		DeliveryDate: "",
		Destination:  "",
		Events:       []*trackingEventRsp{},
	}
}
