// 该模块定义了查询运单跟踪信息的外部接口。
// @Author: Haart
// @Created: 2021-10-27
package rpc

import (
	"fmt"
	"net/http"
	"strings"

	_agent "com.cne/ai-tracking-search/agent"
	_types "com.cne/ai-tracking-search/types"
	_utils "com.cne/ai-tracking-search/utils"

	"time"

	"github.com/gin-gonic/gin"
)

const (
	MaxBatchSize       int   = 30    // 每个原始请求中允许包含的最多运单号。
	MaxSearchQueueSize int64 = 10000 // 查询队列的最大长度。

	TrackingSearchKeyPrefix string = "TRACKING_SEARCH" // 缓存中的查询记录的Key的前缀。
	TrackingQueueKey        string = "TRACKING_QUEUE"  // 查询记录队列Key。

	maxPullCount = 80 // 轮询缓存的最大次数。
)

// 表示查询请求。
type trackingsReq struct {
	CarrierCode string              `json:"carrierCode" binding:"required"` // 运输商代号。
	ClientId    string              `json:"clientId"`                       // 客户端ID。
	Language    _types.LangId       `json:"language"`                       // 期望返回的语言。
	Priority    _types.Priority     `json:"priority"`                       // 优先级(0-2)。
	Token       string              `json:"token"`                          // 和客户端ID对应的鉴权标记。
	Orders      []*trackingOrderReq `json:"orders"`                         // 请求包含的所有待查询运单。
}

// 表示查询请求中的一个运单。
type trackingOrderReq struct {
	TrackingNo string `json:"trackingNo"` // 运单号。
}

type trackingEvents []*trackingEvent

func (s trackingEvents) Len() int           { return len(s) }
func (s trackingEvents) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s trackingEvents) Less(i, j int) bool { return s[i].Date.After(s[j].Date) } // 时间上越晚的事件越小。

// 表示针对一个运单的查询，同时包含查询条件和查询结果。
type trackingSearch struct {
	Src            _types.TrackingResultSrc // 来源。可以是 DB或者API或者CRAWLER
	ClientAddr     string                   // 客户端IP地址。
	ReqTime        time.Time                // 客户端发来请求的时间。
	SeqNo          string                   // 查询流水号。
	CarrierCode    string                   // 运输商编号。
	Language       _types.LangId            // 需要爬取的语言。
	TrackingNo     string                   // 运单号。
	UpdateTime     time.Time                // 最后从查询代理更新的业务时间，即最新的事件时间。
	AgentName      string                   // 查询代理的名字。
	AgentStartTime time.Time                // 启动查询代理的时间。
	AgentEndTime   time.Time                // 查询代理返回的时间。
	Events         trackingEvents           // 事件列表，也就是查询代理返回的有效结果。
	AgentCode      _agent.AgCode            // 查询代理返回的的状态码。
	Err            string                   // 查询代理发生错误时返回的的消息。
	AgentRawText   string                   // 爬取发生错误时返回的原始文本。
	DoneTime       time.Time                // 妥投时间。
	DonePlace      string                   // 妥投的地点。
	Done           bool                     // 是否已经妥投。
}

// 表示跟踪结果的一个事件。
type trackingEvent struct {
	Date    time.Time `json:"date"`   // 事件的时间。
	Details string    `json:"detail"` // 事件的详细描述。
	Place   string    `json:"place"`  // 事件发生的地点。
	State   int       `json:"state"`  // 事件的状态。
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
	Delivered    int                 `json:"delivered"`    // 是否已妥投。1表示已妥投，0表示未妥投。
	DeliveryDate string              `json:"deliveryDate"` // 妥投的时间。
	Destination  string              `json:"destination"`  // 妥投的目的地。
	Cached       bool                `json:"cached"`       // 此响应是否来自于缓存。
	CachedTime   string              `json:"cachedTime"`   // 此响应的缓存时间（UTC）。
	Events       []*trackingEventRsp `json:"events"`       // 运单包含的事件。
}

// 表示查询响应中的事件。
type trackingEventRsp struct {
	Date    string `json:"date"`    // 事件时间。
	State   int    `json:"state"`   // 事件状态。
	Place   string `json:"place"`   // 事件地点。
	Details string `json:"details"` // 事件详细。
}

// 执行运单跟踪状态查询。
func Trackings(ctx *gin.Context) {
	req := trackingsReq{Priority: _types.PriorityLow, Language: _types.LangEN}
	now := time.Now()

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.AbortWithError(400, fmt.Errorf("illegal request: %w", err))
		return
	}

	if len(req.Orders) == 0 {
		ctx.AbortWithError(400, fmt.Errorf("orders cannot be empty"))
		return
	} else if len(req.Orders) > MaxBatchSize {
		ctx.AbortWithError(400, fmt.Errorf("too many orders: [%d]", len(req.Orders)))
		return
	}

	req.CarrierCode = strings.TrimSpace(req.CarrierCode)
	if req.CarrierCode == "" {
		ctx.AbortWithError(400, fmt.Errorf("carrier code cannot be empty"))
		return
	}

	// 为每个运单号构造一个查询对象。
	trackingSearchList := make([]*trackingSearch, 0, len(req.Orders))
	clientAddr := _utils.GetRemoteAddr(ctx.Request)
	for _, order := range req.Orders {
		if seqNo, err := _utils.NewSeqNo(); err != nil {
			// 无法获取新的流水号，处理下一个查询对象。
			continue
		} else {
			trackingSearchList = append(trackingSearchList, &trackingSearch{ReqTime: now, ClientAddr: clientAddr, SeqNo: seqNo, CarrierCode: req.CarrierCode, Language: req.Language, TrackingNo: strings.TrimSpace(order.TrackingNo), Done: false})
		}
	}

	// 从数据库中加载。
	loadTrackingResultFromDb(trackingSearchList)

	// 未妥投的记录（包含数据库中查不到的记录），都需要通过查询代理爬取。
	// 将需要调用查询代理的记录推送到任务队列。
	var trackingSearchList2 []*trackingSearch = make([]*trackingSearch, 0)
	if keys, err := pushTrackingSearchToQueue(_types.Priority(req.Priority), trackingSearchList); err != nil {
		// 推送查询对象到任务队列失败，放弃轮询缓存和拉取查询对象。
	} else {
		// 从缓存拉取查询对象（以及查询结果）。
		if trackingSearchList, err := pullTrackingSearchFromCache(_types.Priority(req.Priority), keys); err == nil {
			// 匹配查询结果。
			matchAllEvents(trackingSearchList)

			// 来自查询代理的查询结果会被保存到数据库。
			go func() {
				defer _utils.RecoverPanic()

				saveTrackingResultToDb(trackingSearchList)
			}()

			trackingSearchList2 = append(trackingSearchList2, trackingSearchList...)
		}
	}

	ctx.JSON(http.StatusOK, buildTrackingsRsp(req.Orders, trackingSearchList, trackingSearchList2))
}

// 构造最终的响应结果。
// orders 待查询的运单。
// r1 来自数据库的响应结果。
// r2 来自查询代理的响应结果。
// 返回组合后的结果。返回结果按照 `orders` 的顺序排序。首先从r2中获取结果，如果不存在则尝试从r1中返回结果，否则返回一个表示未查询到的结果。
func buildTrackingsRsp(orders []*trackingOrderReq, r1, r2 []*trackingSearch) *trackingsRsp {
	data := make([]*trackingOrderRsp, 0, len(orders))

	logList := make([]*trackingSearch, 0)
	for _, orderReq := range orders {
		if ts, ok := findTrackingSearchByTrackingNo(r2, orderReq.TrackingNo); ok {
			data = append(data, buildTrackingOrderResult(ts))
			logList = append(logList, ts)
		} else {
			if ts, ok := findTrackingSearchByTrackingNo(r1, orderReq.TrackingNo); ok {
				data = append(data, buildTrackingOrderResult(ts))
				logList = append(logList, ts)
			} else {
				data = append(data, buildEmptyTrackingOrderResult(ts.TrackingNo))
			}
		}
	}

	go func() {
		defer _utils.RecoverPanic()

		saveLogToDb(logList)
	}()

	result := trackingsRsp{Data: data}
	result.Code = rSuccess
	result.Message = "success"
	result.ErrorId = 0

	return &result
}

// 从查询对象集合中寻找运单号匹配的的查询对象。
// trackingSearchList 查询对象集合。
// trackingNo 待查询的运单号。
func findTrackingSearchByTrackingNo(trackingSearchList []*trackingSearch, trackingNo string) (*trackingSearch, bool) {
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
func buildTrackingOrderResult(trackingSearch *trackingSearch) *trackingOrderRsp {
	events := make([]*trackingEventRsp, 0, len(trackingSearch.Events))
	for _, evt := range trackingSearch.Events {
		events = append(events, &trackingEventRsp{
			Date:    _utils.FormatTime(evt.Date), // TODO: UTC?
			State:   evt.State,
			Place:   evt.Place,
			Details: evt.Details,
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
		Delivered:    0,
		DeliveryDate: "",
		Destination:  "",
		Cached:       cached,
		CachedTime:   cachedTime,
		Events:       events,
	}

	if _agent.IsSuccess(trackingSearch.AgentCode) {
		result.State = 1 // 表示此结果来自于数据库或者查询代理爬取的有效网页内容。
		if trackingSearch.Done {
			result.Delivered = 1
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
		Delivered:    0,
		DeliveryDate: "",
		Destination:  "",
		Events:       []*trackingEventRsp{},
	}
}
