// 该模块定义了查询运单跟踪信息的外部接口。
// @Author: Haart
// @Created: 2021-10-27
package rpc

import (
	"fmt"
	"net/http"
	"strings"

	_crawler "com.cne/ai-tracking-search/crawler"
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

	maxPullCount = 40 // 轮询缓存的最大次数。
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
	Src              _types.TrackingResultSrc // 来源。可以是 DB或者API或者CRAWLER
	ClientAddr       string                   // 客户端IP地址。
	ReqTime          time.Time                // 客户端发来请求的时间。
	SeqNo            string                   // 查询流水号。
	CarrierCode      string                   // 运输商编号。
	Language         _types.LangId            // 需要爬取的语言。
	TrackingNo       string                   // 运单号。
	UpdateTime       time.Time                // 最后从爬虫更新的业务时间，即最新的事件时间。
	CrawlerName      string                   // 爬虫的名字。
	CrawlerStartTime time.Time                // 启动爬虫的时间。
	CrawlerEndTime   time.Time                // 爬虫返回的时间。
	Events           trackingEvents           // 事件列表，也就是爬虫返回的有效结果。
	CrawlerCode      _crawler.CrCode          // 爬虫返回的的状态码。
	Err              string                   // 爬虫发生错误时返回的的消息。
	RawText          string                   // 爬取发生错误时返回的原始文本。
	DoneTime         time.Time                // 妥投时间。
	DonePlace        string                   // 妥投的地点。
	Done             bool                     // 是否已经妥投。
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
	State        int                 `json:"state"`        // 查询状态，即爬虫是否返回了能够解析的查询结果（即使查询结果为空）。
	Message      string              `json:"message"`      // 运单状态对应的文本。
	Delivered    int                 `json:"delivered"`    // 是否已妥投。1表示已妥投，0表示未妥投。
	DeliveryDate string              `json:"deliveryDate"` // 妥投的时间。
	Destination  string              `json:"destination"`  // 妥投的目的地。
	Src          string              `json:"src"`          // 响应的数据来源。
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

	// 未妥投的记录（包含数据库中查不到的记录），都需要通过爬虫爬取。
	// 将需要调用爬虫的记录推送到任务队列。
	var trackingSearchList2 []*trackingSearch = make([]*trackingSearch, 0)
	if keys, err := pushTrackingSearchToQueue(_types.Priority(req.Priority), trackingSearchList); err != nil {
		// 推送查询对象到任务队列失败，放弃轮询缓存和拉取查询对象。
	} else {
		// 从缓存拉取查询对象（以及查询结果）。
		if trackingSearchList, err := pullTrackingSearchFromCache(_types.Priority(req.Priority), keys); err == nil {
			// 匹配查询结果。
			matchAllEvents(trackingSearchList)

			// 来自爬虫的查询结果会被保存到数据库。
			go func() {
				defer _utils.RecoverPanic()

				saveTrackingResultToDb(trackingSearchList)
			}()

			trackingSearchList2 = append(trackingSearchList2, trackingSearchList...)
		}
	}

	ctx.JSON(http.StatusOK, buildResult(req.Orders, trackingSearchList, trackingSearchList2))
}
