// 该模块定义了查询运单跟踪信息的外部接口。
// @Author: Haart
// @Created: 2021-10-27
package rpc

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"

	_cache "com.cne/ai-tracking-search/cache"
	_crawler "com.cne/ai-tracking-search/crawler"
	_db "com.cne/ai-tracking-search/db"
	_queue "com.cne/ai-tracking-search/queue"
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

	trSuccess trackingsRspCode = "S" // 表示成功的查询。
	trError   trackingsRspCode = "E" // 表示出现错误的查询。

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

// 表示针对一个运单的查询。
type trackingSearch struct {
	Src              _types.TrackingResultSrc // 来源。可以是 DB或者API或者CRAWLER
	Host             string                   // 客户端IP地址。
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
	Code    trackingsRspCode    `json:"code"`    // 表示查询状态的代码，如果该字段是`trError`，那么`Data`字段不可用。
	ErrorId int                 `json:"errorId"` // 表示具体错误描述的编码。
	Message string              `json:"message"` // 查询状态代码对应的文本。
	Data    []*trackingOrderRsp `json:"data"`    // 每个运单对应的查询结果。
}

// 响应结果代码。
type trackingsRspCode string

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
	}

	// 为每个运单号构造一个查询对象。
	trackingSearchList := make([]*trackingSearch, 0, len(req.Orders))
	for _, order := range req.Orders {
		if seqNo, err := _utils.NewSeqNo(); err != nil {
			// 无法获取新的流水号，处理下一个查询对象。
			continue
		} else {
			// TODO: 通过Request Header 获取客户端IP地址。
			trackingSearchList = append(trackingSearchList, &trackingSearch{ReqTime: now, Host: "", SeqNo: seqNo, CarrierCode: req.CarrierCode, Language: req.Language, TrackingNo: order.TrackingNo, Done: false})
		}
	}

	// 从数据库中加载。
	loadFromDb(trackingSearchList)

	// 未妥投的记录（包含数据库中查不到的记录），都需要通过爬虫爬取。
	// 将需要调用爬虫的记录推送到任务队列。
	var trackingSearchList2 []*trackingSearch = make([]*trackingSearch, 0)
	if keys, err := pushSearchToQueue(_types.Priority(req.Priority), trackingSearchList); err != nil {
		// 推送查询对象到任务队列失败，放弃轮询缓存和拉取查询对象。
	} else {
		// 从缓存拉取查询对象（以及查询结果）。
		if trackingSearchList, err := pullSearchFromCache(_types.Priority(req.Priority), keys); err == nil {
			// 匹配查询结果。
			if err := matchAllEvents(trackingSearchList); err != nil {
				panic(err)
			}

			go func() {
				if err := saveResultToDb(trackingSearchList); err != nil {
					log.Printf("[WARN] Cannot save result to db. cause=%s\n", err)
				}
			}()

			trackingSearchList2 = append(trackingSearchList2, trackingSearchList...)
		}
	}

	ctx.JSON(http.StatusOK, buildResult(req.Orders, trackingSearchList, trackingSearchList2))
}

// 从数据库中读取跟踪记录。
// trackingSearchList 待读取相应跟踪记录的查询对象。每个对象都要到数据库中查询一次。
func loadFromDb(trackingSearchList []*trackingSearch) error {
	for i, ts := range trackingSearchList {
		if tr, err := _db.QueryTrackingResultByTrackingNo(ts.CarrierCode, ts.Language, ts.TrackingNo); err != nil {
			return err
		} else if tr != nil {
			ts.Src = _types.SrcDB
			ts.UpdateTime = tr.UpdateTime
			if tr.EventsJson != "" {
				json.Unmarshal([]byte(tr.EventsJson), &ts.Events)
			} else {
				ts.Events = []*trackingEvent{}
			}
			ts.CrawlerCode = _crawler.CcSuccess2 // 如果跟踪记录来自于数据库，那么爬虫返回码字段固定为成功，因为该记录必然来自于之前曾经成功的查询。
			ts.Done = tr.Done
			trackingSearchList[i] = ts
		}
	}
	return nil
}

// 将查询对象推送到缓存和队列。
// priority 优先级。
// trackingSearchList 待推送到缓存和队列的查询对象。
func pushSearchToQueue(priority _types.Priority, trackingSearchList []*trackingSearch) ([]string, error) {
	keys := make([]string, 0)

	queueTopic := TrackingQueueKey + "$" + priority.String()

	// 检查查询队列是否已经超长。
	if cl, err := _queue.Length(queueTopic); err != nil {
		return nil, err
	} else {
		if cl+int64(len(trackingSearchList)) > MaxSearchQueueSize {
			return nil, fmt.Errorf("too many searchs")
		}
	}

	avaiableUpdateTime := time.Now().Add(time.Hour * -2)        // 有效更新时间。
	avaiableUpdateTimeOfEmpty := time.Now().Add(time.Hour * -8) // 空单号有效更新时间。

	for _, ts := range trackingSearchList {
		// 数据库中存在跟踪记录，那么检查其它条件，判断是否可以直接使用数据库记录，而不再调用爬虫查询。
		if len(ts.Events) != 0 {
			if ts.Done {
				// 已完成，这种查询对象不再需要执行。
				continue
			} else if priority != _types.PriorityHighest && (ts.UpdateTime.After(avaiableUpdateTime) || (len(ts.Events) == 0 && ts.UpdateTime.After(avaiableUpdateTimeOfEmpty))) {
				// 未完成，但是当前优先级不是最高，并且满足以下两个条件之一：
				// 1. 更新时间晚于有效更新时间（即数据比较新）;
				// 2. 更新时间晚于有效更新时间2，并且之前查询结果是空单号;
				// 这种查询对象也不再需要执行。
				continue
			}
		}

		// 查询对象保存到缓存。
		key := TrackingSearchKeyPrefix + "$" + ts.SeqNo

		// 如果20秒内该查询对象尚未被爬虫执行则放弃。
		if err := _cache.SetAndExpire(key, map[string]interface{}{"reqTime": _utils.FormatTime(ts.ReqTime), "carrierCode": ts.CarrierCode, "language": ts.Language.String(), "trackingNo": ts.TrackingNo, "status": -1}, time.Second*26); err != nil {
			panic(err)
		}

		// 推送到队列。
		_queue.Push(queueTopic, key)
		keys = append(keys, key)
	}

	return keys, nil
}

// 从缓存中拉取已完成的查询对象。
// 此方法会阻塞，并不断轮询查询对象。直到所有的查询对象状态都变为已有结果或者超时。
// priority 查询对象的优先级。
// keys 查询对象的键集合。
// 返回缓存中的查询对象。
func pullSearchFromCache(priority _types.Priority, keys []string) ([]*trackingSearch, error) {
	result := make([]*trackingSearch, 0, len(keys))

	// 全部查询成功或者重试次数太多则停止重试。
	c := 0
	for {
		// 收集已完成的响应。
		pc := 0
		for _, key := range keys {
			if os, err := _cache.Get(key, "reqTime", "carrierCode", "language", "trackingNo", "status", "crawlerResult", "crawlerName", "crawlerStartTime", "crawlerEndTime"); err != nil {
				return nil, fmt.Errorf("cannot get tracking-search(key=%s) from cache. cause=%w", key, err)
			} else {
				// 爬虫执行状态，该值由爬虫调度程序写入，和数据库中的`status`字段无关。
				status := _utils.AsInt(os[4], -1)
				if status < 1 && c < maxPullCount {
					// 如果返回码是-1或者0，说明爬虫尚未返回结果。
					keys[pc] = key
					pc++
					continue
				}

				_cache.Del(key)

				crawlerRspJson := strings.TrimSpace(_utils.AsString(os[5]))
				trackingResult := _crawler.TrackingResult{Code: _crawler.CcTimeout}
				events := make([]*trackingEvent, 0)
				if crawlerRspJson == "" {
					log.Printf("[WARN] Cannot parse empty crawler result json\n")
				} else {
					crawlerRspJsonBytes := []byte(crawlerRspJson)
					if err := json.Unmarshal(crawlerRspJsonBytes, &trackingResult); err != nil {
						// 首先尝试将爬虫返回的json反序列化为跟踪结果对象。
						// 如果失败，那么尝试反序列化为批量跟踪结果对象。
						// 如果仍然失败则报错。
						// 如果反序列化的批量跟踪结果对象包含的运单记录超过1个，也报错。
						crawlerRsp := _crawler.ResponseWrapper{}
						if err := json.Unmarshal(crawlerRspJsonBytes, &crawlerRsp); err != nil {
							log.Printf("[WARN] Cannot parse crawler result json: %v. cause=%s\n", crawlerRspJson, err)
						} else if len(crawlerRsp.Items) != 1 {
							log.Printf("[WARN] Length of crawler result should be just 1, but %#v\n", crawlerRsp)
						} else {
							trackingResult = crawlerRsp.Items[0]
						}
					}

					// 将爬虫的事件列表映射为待匹配的事件。
					for _, te := range trackingResult.TrackingEventList {
						events = append(events, &trackingEvent{
							Date:    _utils.ParseTime(te.Date), // TODO: 此处是否应当使用ParseUTCTime。
							Details: te.Details,
							Place:   te.Place,
							State:   0,
						})
					}
				}

				language, _ := _types.ParseLangId(_utils.AsString(os[2]))

				trackingSearch := trackingSearch{
					SeqNo:            key[len(TrackingSearchKeyPrefix)+1:],
					ReqTime:          _utils.AsTime(os[0]),
					Src:              _types.SrcCrawler,
					CarrierCode:      _utils.AsString(os[1]),
					Language:         language,
					TrackingNo:       _utils.AsString(os[3]),
					CrawlerName:      _utils.AsString(os[6]),
					CrawlerStartTime: _utils.AsTime(os[7]),
					CrawlerEndTime:   _utils.AsTime(os[8]),
					Events:           events,
					CrawlerCode:      trackingResult.Code,
					Err:              trackingResult.CodeMg,
					RawText:          trackingResult.CMess,
				}

				result = append(result, &trackingSearch)
			}
		} // end of for-key

		// 删除这些已完成的响应。
		keys = keys[:pc]

		if len(keys) == 0 || c >= maxPullCount {
			break
		}

		c++
		// if c <= 1 {
		// 	time.Sleep(time.Second * 4)
		// } else if c <= 3 {
		// 	time.Sleep(time.Second * 3)
		// } else if c <= 5 {
		// 	time.Sleep(time.Second * 2)
		// } else if c <= 7 {
		if c <= 7 {
			time.Sleep(time.Millisecond * 500)
		} else {
			time.Sleep(time.Millisecond * 300)
		}
	}

	return result, nil
}

// 匹配查询对象集合中包含的事件。
// trackingSearchList 待匹配的查询对象集合。
func matchAllEvents(trackingSearchList []*trackingSearch) error {
	for i, ts := range trackingSearchList {
		if rules, err := _db.QueryMatchRuleByCarrierCode(ts.CarrierCode, ts.ReqTime); err != nil {
			return fmt.Errorf("cannot query match rule of carrier(code=%s). cause=%w", ts.CarrierCode, err)
		} else {
			matchEvents(rules, ts)
			trackingSearchList[i] = ts
		}
	}

	return nil
}

// 匹配查询对象中的事件。
// rules 关联的匹配规则。
// 待匹配的查询对象。
func matchEvents(rules []*_db.MatchRulePo, ts *trackingSearch) {
	// 按事件排序。
	sort.Stable(ts.Events)

	for i, evt := range ts.Events {
		// 两次遍历，第一次针对爬虫类别的规则进行匹配。
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

func saveResultToDb(trackingSearchList []*trackingSearch) error {
	now := time.Now()
	for _, ts := range trackingSearchList {
		var eventsJson string
		if ts.Events == nil || len(ts.Events) == 0 {
			eventsJson = ""
		} else {
			if eventsJsonBytes, err := json.Marshal(ts.Events); err != nil {
				panic(err)
			} else {
				eventsJson = string(eventsJsonBytes)
			}
		}

		if carrierPo, err := _db.QueryCarrierByCode(ts.CarrierCode); err != nil {
			return err
		} else {
			if _, err := _db.SaveTrackingResultToDb(carrierPo.Id, ts.Language, ts.TrackingNo, eventsJson, now, ts.Done); err != nil {
				return err
			}
			if trackingId, err := _db.SaveTrackingToDb(carrierPo.Id, ts.Language, ts.TrackingNo, ts.DoneTime, ts.DonePlace, ts.Src, ts.CrawlerName, now, ts.Done); err != nil {
				return err
			} else {
				for _, event := range ts.Events {
					if _, err := _db.SaveTrackingDetailToDb(trackingId, event.Date, event.Place, event.Details, event.State, now); err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func saveLogToDb(trackingSearchList []*trackingSearch) error {
	now := time.Now()
	operator := "auto"
	for _, ts := range trackingSearchList {
		if carrierPo, err := _db.QueryCarrierByCode(ts.CarrierCode); err != nil {
			return err
		} else {
			matchType := 2 // 外部接口指定carrierCode。
			resultStatus := 0
			resultNote := ""
			if ts.CrawlerCode == _crawler.CcSuccess || ts.CrawlerCode == _crawler.CcSuccess2 {
				resultStatus = 1
				resultNote = "查询成功"
			} else if ts.CrawlerCode == _crawler.CcNoTracking {
				resultStatus = 1
				resultNote = "未查询到单号"
			} else if ts.CrawlerCode == _crawler.CcParseFailed {
				resultNote = "无法解析目标网站页面"
			} else if ts.CrawlerCode == _crawler.CcTimeout {
				resultNote = "查询目标网站超时"
			} else {
				resultNote = "未知错误"
			}
			if _crawler.IsSuccess(ts.CrawlerCode) {
				resultStatus = 1
			}
			var timing int64
			if ts.CrawlerEndTime.Before(ts.CrawlerStartTime) {
				timing = int64(^uint64(0) >> 1) // 最大整数。
			} else {
				timing = ts.CrawlerEndTime.Sub(ts.CrawlerStartTime).Milliseconds()
			}
			if _, err := _db.SaveTrackingLogToDb(carrierPo.Id, ts.TrackingNo, matchType, carrierPo.CountryId, int(timing), ts.Host, resultStatus, now, ts.Src, now, operator,
				ts.ReqTime, ts.CrawlerStartTime, ts.CrawlerEndTime, ts.RawText, resultNote); err != nil {
				return err
			}
		}
	}
	return nil
}

// 构造最终的响应结果。
// orders 待查询的运单。
// r1 来自数据库的响应结果。
// r2 来自爬虫的响应结果。
// 返回组合后的结果。返回结果按照 `orders` 的顺序排序。首先从r2中获取结果，如果不存在则尝试从r1中返回结果，否则返回一个表示未查询到的结果。
func buildResult(orders []*trackingOrderReq, r1, r2 []*trackingSearch) *trackingsRsp {
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
		if err := saveLogToDb(logList); err != nil {
			fmt.Printf("[WARN] Cannot save log to db. cause=%s\n", err)
		}
	}()

	return &trackingsRsp{
		Code:    trSuccess,
		Message: "success",
		ErrorId: 0,
		Data:    data,
	}
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
			Date:    _utils.FormatTime(evt.Date.UTC()),
			State:   evt.State,
			Place:   evt.Place,
			Details: evt.Details,
		})
	}

	result := trackingOrderRsp{
		TrackingNo:   trackingSearch.TrackingNo,
		SeqNo:        trackingSearch.SeqNo,
		State:        1, // 表示此结果来自于数据库或者爬虫爬取的有效网页内容。
		Message:      "",
		Delivered:    0,
		DeliveryDate: "",
		Destination:  "",
		Src:          trackingSearch.Src.String(),
		Events:       events,
	}

	if trackingSearch.Done {
		result.Delivered = 1
		result.DeliveryDate = _utils.FormatTime(trackingSearch.DoneTime)
		result.Destination = trackingSearch.DonePlace
	}

	if !_crawler.IsSuccess(trackingSearch.CrawlerCode) {
		result.State = 0 // 表示爬虫发去的网页内容无效（不存在或者无法解析）。
		result.Message = trackingSearch.Err
	}

	return &result
}

// 构造一个空的表示无效的跟踪记录。
// TMS 调用端要求返回结果中的运单记录和请求中的运单记录数量、顺序保持一致。
// 如果既不能从数据库，也不能从爬虫获取跟踪结果，那么调用此方法生成一个。
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
