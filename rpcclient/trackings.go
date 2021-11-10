package rpcclient

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	_agent "com.cne/ai-tracking-search/agent"
	_cache "com.cne/ai-tracking-search/cache"
	_queue "com.cne/ai-tracking-search/queue"
	_types "com.cne/ai-tracking-search/types"
	_utils "com.cne/ai-tracking-search/utils"
)

const (
	trackingSearchKeyPrefix string = "TRACKING_SEARCH" // 缓存中的查询记录的Key的前缀。
	trackingQueueKey        string = "TRACKING_QUEUE"  // 查询记录队列Key。

	maxSearchQueueSize int64 = 10000 // 查询队列的最大长度。
	maxPullCount       int   = 80    // 轮询缓存的最大次数。
)

// 表示针对一个运单的查询，同时包含查询条件和查询结果。
type TrackingSearch struct {
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
	Events         TrackingEvents           // 事件列表，也就是查询代理返回的有效结果。
	AgentCode      _agent.AgCode            // 查询代理返回的的状态码。
	Err            string                   // 查询代理发生错误时返回的的消息。
	AgentRawText   string                   // 爬取发生错误时返回的原始文本。
	DoneTime       time.Time                // 妥投时间。
	DonePlace      string                   // 妥投的地点。
	Done           bool                     // 是否已经妥投。
}

// 表示跟踪结果的一个事件。
// 此处的结构必须和爬虫返回结果匹配。
type TrackingEvent struct {
	Date    time.Time `json:"date"`   // 事件的时间。
	Details string    `json:"detail"` // 事件的详细描述。
	Place   string    `json:"place"`  // 事件发生的地点。
	State   int       `json:"state"`  // 事件的状态。
}

type TrackingEvents []*TrackingEvent

func (s TrackingEvents) Len() int           { return len(s) }
func (s TrackingEvents) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s TrackingEvents) Less(i, j int) bool { return s[i].Date.After(s[j].Date) } // 时间上越晚的事件越小。

// 将查询对象推送到缓存和队列。
// priority 优先级。
// trackingSearchList 待推送到缓存和队列的查询对象。
func PushTrackingSearchToQueue(priority _types.Priority, trackingSearchList []*TrackingSearch) ([]string, error) {
	keys := make([]string, 0)

	queueTopic := trackingQueueKey + "$" + priority.String()

	// 检查查询队列是否已经超长。
	if cl, err := _queue.Length(queueTopic); err != nil {
		return nil, err
	} else {
		if cl+int64(len(trackingSearchList)) > maxSearchQueueSize {
			return nil, fmt.Errorf("too many searchs")
		}
	}

	avaiableUpdateTime := time.Now().Add(time.Hour * -2)        // 有效更新时间。
	avaiableUpdateTimeOfEmpty := time.Now().Add(time.Hour * -8) // 空单号有效更新时间。

	for _, ts := range trackingSearchList {
		// 跳过空单号，这种查询请求是不合法的。
		if ts.TrackingNo == "" {
			continue
		}

		// 数据库中存在跟踪记录，那么检查其它条件，判断是否可以直接使用数据库记录，而不再调用查询代理查询。
		if len(ts.Events) != 0 {
			// 优先级最高的情况下，必须调用查询代理。
			if priority != _types.PriorityHighest {
				if ts.Done {
					// 已完成，这种查询对象不再需要执行。
					continue
				} else if ts.UpdateTime.After(avaiableUpdateTime) || (len(ts.Events) == 0 && ts.UpdateTime.After(avaiableUpdateTimeOfEmpty)) {
					// 未完成，并且满足以下两个条件之一：
					// 1. 更新时间晚于有效更新时间（即数据比较新）;
					// 2. 更新时间晚于有效更新时间2，并且之前查询结果是空单号;
					// 这种查询对象也不再需要执行。
					continue
				}
			}
		}

		// 查询对象保存到缓存。
		key := trackingSearchKeyPrefix + "$" + ts.SeqNo

		// 如果20秒内该查询对象尚未被查询代理执行则放弃。
		if err := _cache.SetAndExpire(key, map[string]interface{}{"reqTime": _utils.AsString(ts.ReqTime), "carrierCode": ts.CarrierCode, "language": ts.Language.String(), "trackingNo": ts.TrackingNo, "clientAddr": ts.ClientAddr, "status": -1}, 60*time.Second); err != nil {
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
func PullTrackingSearchFromCache(priority _types.Priority, keys []string) ([]*TrackingSearch, error) {
	result := make([]*TrackingSearch, 0, len(keys))

	// 全部查询成功或者重试次数太多则停止重试。
	c := 0
	for {
		// 收集已完成的响应。
		pc := 0
		for _, key := range keys {
			if os, err := _cache.Get(key, "status", "reqTime", "carrierCode", "language", "trackingNo", "clientAddr", "agentSrc", "agentErr", "agentResult", "agentName", "agentStartTime", "agentEndTime"); err != nil {
				return nil, fmt.Errorf("cannot get tracking-search(key=%s) from cache. cause=%w", key, err)
			} else {
				// 查询代理执行状态，该值由查询代理调度程序写入，和数据库中的`status`字段无关。
				status := _utils.AsInt(os[0], -1)
				if status < 1 && c < maxPullCount {
					// 如果返回码是-1或者0，说明查询代理尚未返回结果。
					keys[pc] = key
					pc++
					continue
				}

				_cache.Del(key)

				reqTime := _utils.AsTime(os[1])
				carrierCode := _utils.AsString(os[2])
				language, _ := _types.ParseLangId(_utils.AsString(os[3]))
				trackingNo := _utils.AsString(os[4])
				clientAddr := _utils.AsString(os[5])
				agentSrc := _types.TrackingResultSrc(_utils.AsInt(os[6], int(_types.SrcUnknown)))
				agentErr := _utils.AsString(os[7])
				agentRspJson := strings.TrimSpace(_utils.AsString(os[8]))
				agentName := _utils.AsString(os[9])
				agentStartTime := _utils.AsTime(os[10])
				agentEndTime := _utils.AsTime(os[11])

				trackingResult := _agent.TrackingResult{Code: _agent.AcTimeout}
				agentCode := _agent.AcTimeout
				message := ""
				events := make([]*TrackingEvent, 0)
				if agentRspJson == "" {
					log.Printf("[WARN] Cannot parse empty crawler result json\n")
				} else {
					crawlerRspJsonBytes := []byte(agentRspJson)
					if err := json.Unmarshal(crawlerRspJsonBytes, &trackingResult); err != nil {
						// 首先尝试将查询代理返回的json反序列化为跟踪结果对象。
						// 如果失败，那么尝试反序列化为批量跟踪结果对象。
						// 如果仍然失败则报错。
						// 如果反序列化的批量跟踪结果对象包含的运单记录超过1个，也报错。
						crawlerRsp := _agent.ResponseWrapper{}
						if err := json.Unmarshal(crawlerRspJsonBytes, &crawlerRsp); err != nil {
							agentCode = _agent.AcParseFailed
							log.Printf("[WARN] Cannot parse crawler result json: %v. cause=%s\n", agentRspJson, err)
						} else if len(crawlerRsp.Items) != 1 {
							agentCode = _agent.AcOther
							log.Printf("[WARN] Length of crawler result should be just 1, but %#v\n", crawlerRsp)
						} else {
							trackingResult = crawlerRsp.Items[0]
							if v, err := strconv.Atoi(crawlerRsp.Code); err != nil {
								agentCode = _agent.AcParseFailed
							} else {
								agentCode = _agent.AgCode(v)
							}
							message = crawlerRsp.Message
						}
					} else {
						agentCode = trackingResult.Code
						message = trackingResult.CMess
					}

					// 将查询代理的事件列表映射为待匹配的事件。
					for _, te := range trackingResult.TrackingEventList {
						events = append(events, &TrackingEvent{
							Date:    _utils.ParseTime(te.Date), // TODO: 此处是否应当使用ParseUTCTime。
							Details: te.Details,
							Place:   te.Place,
							State:   0,
						})
					}
				}

				// 此处忽略trackingResult.CodeMg，该字段似乎已经弃用。

				if agentErr == "" {
					// 如果调用代理时没有出现错误，那么从代理的响应结果中获取错误信息。
					agentErr = message
				}

				trackingSearch := TrackingSearch{
					SeqNo:          key[len(trackingSearchKeyPrefix)+1:],
					ReqTime:        reqTime,
					Src:            agentSrc,
					CarrierCode:    carrierCode,
					Language:       language,
					TrackingNo:     trackingNo,
					ClientAddr:     clientAddr,
					AgentName:      agentName,
					AgentStartTime: agentStartTime,
					AgentEndTime:   agentEndTime,
					Events:         events,
					AgentCode:      agentCode,
					Err:            agentErr,
					AgentRawText:   agentRspJson,
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
