package rpc

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	_agent "com.cne/ai-tracking-search/agent"
	_cache "com.cne/ai-tracking-search/cache"
	_db "com.cne/ai-tracking-search/db"
	_queue "com.cne/ai-tracking-search/queue"
	_types "com.cne/ai-tracking-search/types"
	_utils "com.cne/ai-tracking-search/utils"
)

// 从数据库中读取跟踪记录。
// trackingSearchList 待读取相应跟踪记录的查询对象。每个对象都要到数据库中查询一次。
func loadTrackingResultFromDb(trackingSearchList []*trackingSearch) {
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
				ts.Events = []*trackingEvent{}
			}
			ts.AgentCode = _agent.AcSuccess2 // 如果跟踪记录来自于数据库，那么查询代理返回码字段固定为成功，因为该记录必然来自于之前曾经成功的查询。
			ts.Done = tr.Done
			trackingSearchList[i] = ts
		}
	}
}

// 将查询对象推送到缓存和队列。
// priority 优先级。
// trackingSearchList 待推送到缓存和队列的查询对象。
func pushTrackingSearchToQueue(priority _types.Priority, trackingSearchList []*trackingSearch) ([]string, error) {
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
		// 跳过空单号，这种查询请求是不合法的。
		if ts.TrackingNo == "" {
			continue
		}

		// 数据库中存在跟踪记录，那么检查其它条件，判断是否可以直接使用数据库记录，而不再调用查询代理查询。
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

		// 如果20秒内该查询对象尚未被查询代理执行则放弃。
		if err := _cache.SetAndExpire(key, map[string]interface{}{"reqTime": _utils.FormatTime(ts.ReqTime), "carrierCode": ts.CarrierCode, "language": ts.Language.String(), "trackingNo": ts.TrackingNo, "clientAddr": ts.ClientAddr, "status": -1}, time.Second*26); err != nil {
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
func pullTrackingSearchFromCache(priority _types.Priority, keys []string) ([]*trackingSearch, error) {
	result := make([]*trackingSearch, 0, len(keys))

	// 全部查询成功或者重试次数太多则停止重试。
	c := 0
	for {
		// 收集已完成的响应。
		pc := 0
		for _, key := range keys {
			if os, err := _cache.Get(key, "reqTime", "carrierCode", "language", "trackingNo", "clientAddr", "status", "agentErr", "agentResult", "agentName", "agentStartTime", "agentEndTime"); err != nil {
				return nil, fmt.Errorf("cannot get tracking-search(key=%s) from cache. cause=%w", key, err)
			} else {
				// 查询代理执行状态，该值由查询代理调度程序写入，和数据库中的`status`字段无关。
				status := _utils.AsInt(os[5], -1)
				if status < 1 && c < maxPullCount {
					// 如果返回码是-1或者0，说明查询代理尚未返回结果。
					keys[pc] = key
					pc++
					continue
				}

				_cache.Del(key)

				agentErr := _utils.AsString(os[6])
				agentRspJson := strings.TrimSpace(_utils.AsString(os[7]))
				trackingResult := _agent.TrackingResult{Code: _agent.AcTimeout}
				agentCode := _agent.AcTimeout
				message := ""
				events := make([]*trackingEvent, 0)
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
							log.Printf("[WARN] Cannot parse crawler result json: %v. cause=%s\n", agentRspJson, err)
						} else if len(crawlerRsp.Items) != 1 {
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
						events = append(events, &trackingEvent{
							Date:    _utils.ParseTime(te.Date), // TODO: 此处是否应当使用ParseUTCTime。
							Details: te.Details,
							Place:   te.Place,
							State:   0,
						})
					}

					fmt.Printf("%s\n", agentRspJson)
					fmt.Printf("%d\n", trackingResult.Code)
				}

				language, _ := _types.ParseLangId(_utils.AsString(os[2]))

				// 此处忽略trackingResult.CodeMg，该字段似乎已经弃用。

				if agentErr == "" {
					// 如果调用代理时没有出现错误，那么从代理的响应结果中获取错误信息。
					agentErr = message
				}

				trackingSearch := trackingSearch{
					SeqNo:          key[len(TrackingSearchKeyPrefix)+1:],
					ReqTime:        _utils.AsTime(os[0]),
					Src:            _types.SrcCrawler,
					CarrierCode:    _utils.AsString(os[1]),
					Language:       language,
					TrackingNo:     _utils.AsString(os[3]),
					ClientAddr:     _utils.AsString(os[4]),
					AgentName:      _utils.AsString(os[8]),
					AgentStartTime: _utils.AsTime(os[9]),
					AgentEndTime:   _utils.AsTime(os[10]),
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

// 匹配查询对象集合中包含的事件。
// trackingSearchList 待匹配的查询对象集合。
func matchAllEvents(trackingSearchList []*trackingSearch) {
	for i, ts := range trackingSearchList {
		rules := _db.QueryMatchRuleByCarrierCode(ts.CarrierCode, ts.ReqTime)

		matchEvents(rules, ts)
		trackingSearchList[i] = ts
	}
}

// 匹配查询对象中的事件。
// rules 关联的匹配规则。
// 待匹配的查询对象。
func matchEvents(rules []*_db.MatchRulePo, ts *trackingSearch) {
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

func saveTrackingResultToDb(trackingSearchList []*trackingSearch) {
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
			_db.SaveTrackingResultToDb(carrierPo.Id, ts.Language, ts.TrackingNo, eventsJson, now, ts.Done)
			trackingId := _db.SaveTrackingToDb(carrierPo.Id, ts.Language, ts.TrackingNo, ts.DoneTime, ts.DonePlace, ts.Src, ts.AgentName, now, ts.Done)
			for _, event := range ts.Events {
				_db.SaveTrackingDetailToDb(trackingId, event.Date, event.Place, event.Details, event.State, now)
			}
		}
	}
}

func saveLogToDb(trackingSearchList []*trackingSearch) {
	now := time.Now()
	operator := "auto"
	for _, ts := range trackingSearchList {
		matchType := 2 // 外部接口指定carrierCode。
		resultStatus := 0
		resultNote := ""
		if ts.AgentCode == _agent.AcSuccess || ts.AgentCode == _agent.AcSuccess2 {
			resultStatus = 1
			resultNote = "查询成功"
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
		if ts.AgentEndTime.Before(ts.AgentStartTime) {
			timing = int64(^uint64(0) >> 1) // 最大整数。
		} else {
			timing = ts.AgentEndTime.Sub(ts.AgentStartTime).Milliseconds()
		}

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

// 构造最终的响应结果。
// orders 待查询的运单。
// r1 来自数据库的响应结果。
// r2 来自查询代理的响应结果。
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
			Date:    _utils.FormatTime(evt.Date.UTC()),
			State:   evt.State,
			Place:   evt.Place,
			Details: evt.Details,
		})
	}

	result := trackingOrderRsp{
		TrackingNo:   trackingSearch.TrackingNo,
		SeqNo:        trackingSearch.SeqNo,
		Message:      "",
		Delivered:    0,
		DeliveryDate: "",
		Destination:  "",
		Src:          trackingSearch.Src.String(),
		Events:       events,
	}

	if _agent.IsSuccess(trackingSearch.AgentCode) {
		result.State = 1 // 表示此结果来自于数据库或者查询代理爬取的有效网页内容。
		if trackingSearch.Done {
			result.Delivered = 1
			result.DeliveryDate = _utils.FormatTime(trackingSearch.DoneTime)
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
