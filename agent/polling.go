// 该模块定义了轮询查询代理的方法。
// @Author: Haart
// @Created: 2021-10-27
// TODO: 该模块可以被提取为单独的应用。
package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"net/http"

	_cache "com.cne/ai-tracking-search/cache"
	_db "com.cne/ai-tracking-search/db"
	_queue "com.cne/ai-tracking-search/queue"
	_types "com.cne/ai-tracking-search/types"
	_utils "com.cne/ai-tracking-search/utils"
	"github.com/go-redis/redis"
)

type agentResult struct {
	StartTime time.Time // 调用查询代理的时间。
	EndTime   time.Time // 查询代理返回响应的时间。
	Result    string    // 查询代理返回的内容。
}

const (
	TrackingSearchKeyPrefix = "TRACKING_SEARCH" // 缓存中的查询记录的Key的前缀。
	TrackingQueueKey        = "TRACKING_QUEUE"  // 查询记录队列Key。

	ctPython = "PYTHON"
	ctJava   = "JAVA"
	ctGo     = "GO"
)

var (
	allPriorities []_types.Priority // 所有消息队列的主题。

	pollingBatchSize int // 轮询时每次处理的批量。
)

func init() {
	allPriorities = []_types.Priority{_types.PriorityHighest, _types.PriorityHigh, _types.PriorityLow}
}

func InitAgent(pollingBatchSize_ int) error {
	if pollingBatchSize_ <= 4 {
		return fmt.Errorf("polling batch size should larger than 4, but %d", pollingBatchSize_)
	}
	if pollingBatchSize_ > 5000 {
		return fmt.Errorf("polling batch size should not larger than 5000, but %d", pollingBatchSize_)
	}

	pollingBatchSize = pollingBatchSize_

	return nil
}

// 启动轮询。
func PollForEver() {
	chs := make([]chan int, pollingBatchSize)
	cases := make([]reflect.SelectCase, pollingBatchSize)
	for i := 0; i < pollingBatchSize; i++ {
		ch := make(chan int)
		chs[i] = ch
		cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
		go func() { ch <- 1 }() // 将协程标记为就绪。
	}

	doPollOne := func(i int) {
		go func() {
			defer func() {
				_utils.RecoverPanic()

				chs[i] <- 1 // 将协程标记为就绪。
			}()
			pollOne()
			time.Sleep(400 * time.Millisecond) // 喘口气 ^_^
		}()
	}

	// 轮询，任何一个协程就绪，就启动。
	for {
		i, _, _ := reflect.Select(cases)
		doPollOne(i)
	}
}

func pollOne() {
	// 依次从不同的优先级队列中获取任务。
	p, key := nextKey()

	if p != -1 {
		seqNo := key[len(TrackingSearchKeyPrefix)+1:]

		if os, err := _cache.Get(key, "reqTime", "carrierCode", "language", "trackingNo"); err != nil {
			log.Printf("[ERROR] Cannot get tracking-search(key=%s) from cache. cause=%s\n", key, err)
			updateCache(key, _types.SrcUnknown, "", fmt.Sprintf("$缓存不可用(seq-no=%s)$", seqNo), &agentResult{})
		} else if os[0] == nil && os[1] == nil && os[2] == nil && os[3] == nil {
			log.Printf("[ERROR] Cannot get tracking-search(key=%s) from cache\n", key)
			updateCache(key, _types.SrcUnknown, "", fmt.Sprintf("$缓存丢失查询对象(seq-no=%s)$", seqNo), &agentResult{})
		} else {
			reqTime := _utils.AsTime(os[0])
			carrierCode := _utils.AsString(os[1])

			var language _types.LangId
			if v, err := _types.ParseLangId(_utils.AsString(os[2])); err != nil {
				log.Printf("[WARN] Illegal language: %v\n", os[2])
			} else {
				language = v
			}
			trackingNo := _utils.AsString(os[3])

			// 尝试找API，如果找不到API，那么找爬虫。
			apiInfo := _db.QueryApiInfoByCarrierCode(carrierCode, reqTime)
			if apiInfo != nil {
				apiParams := _db.QueryApiParamsByApiId(apiInfo.Id)
				callApi(key, apiInfo, apiParams, seqNo, carrierCode, language, trackingNo)
			} else {
				// 查询对应的查询代理和参数。
				crawlerInfo := _db.QueryCrawlerInfoByCarrierCode(carrierCode, reqTime)

				if crawlerInfo != nil {
					callCrawler(key, crawlerInfo, seqNo, carrierCode, language, trackingNo)
				} else {
					log.Printf("[WARN] Cannot find suitable agent for carrier[%s] at %s\n", carrierCode, reqTime)
					updateCache(key, _types.SrcUnknown, "", fmt.Sprintf("$没有匹配到查询代理(carrier-code=%s)$", carrierCode), &agentResult{})
				}
			}
		}
	}
}

func nextKey() (_types.Priority, string) {
	for _, p := range allPriorities {
		if result, err := _queue.Pop(TrackingQueueKey + "$" + p.String()); err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			} else {
				panic(err)
			}
		} else {
			return p, result
		}
	}

	return -1, ""
}

func callApi(key string, apiInfo *_db.ApiInfoPo, apiParams []*_db.ApiParamPo, seqNo, carrierCode string, language _types.LangId, trackingNo string) {
	_cache.Update(key, map[string]interface{}{"status": 0})

	url := apiInfo.Url + "/fetchTrackInfoList"

	data := map[string]interface{}{
		"trackingNo": trackingNo,
	}

	reqData := map[string]interface{}{}

	for _, ap := range apiParams {
		if ap.FieldName == "reqUrl" {
			data["reqUrl"] = ap.FieldValue
		} else if ap.FieldName == "siteAnalyzedName" {
			data["siteAnalyzedName"] = ap.FieldValue
		} else if ap.FieldName == "siteCrawlingName" {
			data["siteCrawlingName"] = ap.FieldValue
		} else if ap.FieldName == "reqProxy" {
			data["reqProxy"] = ap.FieldValue
		} else if ap.FieldName == "reqTimeout" {
			data["reqTimeout"] = _utils.AsInt(ap.FieldValue, 25)
		} else {
			if strings.Contains(ap.FieldValue, "{lan}") {
				ap.FieldValue = strings.ReplaceAll(ap.FieldValue, "{lan}", strings.ToLower(language.String()))
			}
			reqData[ap.FieldName] = ap.FieldValue
		}
	}

	reqDataJson, _ := json.Marshal(reqData)
	data["reqData"] = string(reqDataJson)

	var dataJson string
	if v, err := json.Marshal(data); err != nil {
		panic(fmt.Errorf("cannot convert crawler params to json, cause=%w", err))
	} else {
		dataJson = string(v)
	}

	// TODO: 不使用Python，直接调用API。
	log.Printf("[DEBUG] API by python processing {seq-no: %s, carrier-code: %s, tracking-no: %s} from %s [data=%s]\n", seqNo, carrierCode, trackingNo, url, dataJson)

	// 固定使用POST方式调用Python查询代理。
	aResult := &agentResult{StartTime: time.Now()}

	if rsp, err := http.Post(url, "application/json", strings.NewReader(dataJson)); err != nil {
		// 查询代理不可用。
		log.Printf("[WARN]: Cannot call api {api-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}. cause=%s",
			apiInfo.Name, carrierCode, language.String(), trackingNo, seqNo, err)
		updateCache(key, _types.SrcCrawler, apiInfo.Name, fmt.Sprintf("$调用API失败(carrier-code=%s,api-name=%s)$", carrierCode, apiInfo.Name), &agentResult{})
	} else {
		buf := strings.Builder{}
		if _, err := io.Copy(&buf, rsp.Body); err != nil {
			log.Printf("[WARN]: Cannot read response from api {api-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
				apiInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
		} else {
			aResult.EndTime = time.Now()
			aResult.Result = strings.ReplaceAll(strings.ReplaceAll(buf.String(), "'", "\""), "None", "\"\"") // Python查询代理返回的json格式字符串不合规，需要兼容。

			updateCache(key, _types.SrcAPI, apiInfo.Name, "", aResult)
		}
	}
}

func callCrawler(key string, crawlerInfo *_db.CrawlerInfoPo, seqNo, carrierCode string, language _types.LangId, trackingNo string) {
	_cache.Update(key, map[string]interface{}{"status": 0})

	var aResult *agentResult
	var cErr error
	if crawlerInfo.Type == ctPython {
		// 调用python查询代理。
		aResult, cErr = callCrawlerByPython(crawlerInfo, seqNo, carrierCode, language, trackingNo)
		if cErr != nil {
			log.Printf("[WARN]: Cannot call python crawler. cause=%s\n", cErr)
			updateCache(key, _types.SrcCrawler, crawlerInfo.Name, fmt.Sprintf("$调用Python爬虫失败(carrier-code=%s,crawler-name=%s)$", carrierCode, crawlerInfo.Name), &agentResult{})
		}
	} else if crawlerInfo.Type == ctGo {
		// 调用Go查询代理。
		aResult, cErr = callCrawlerByGolang(crawlerInfo, seqNo, carrierCode, language, trackingNo)
		if cErr != nil {
			log.Printf("[WARN]: Cannot call golang crawler. cause=%s\n", cErr)
			updateCache(key, _types.SrcCrawler, crawlerInfo.Name, fmt.Sprintf("$调用GO爬虫失败(carrier-code=%s,crawler-name=%s)$", carrierCode, crawlerInfo.Name), &agentResult{})
		}
	} else {
		log.Printf("[WARN] Unsupported crawler type: %s\n", crawlerInfo.Type)
		updateCache(key, _types.SrcCrawler, crawlerInfo.Name, fmt.Sprintf("$不支持的爬虫类型(carrier-code=%s,crawler-name=%s,crawler-type=%s)$", carrierCode, crawlerInfo.Name, crawlerInfo.Type), &agentResult{})
	}

	if aResult != nil {
		updateCache(key, _types.SrcCrawler, crawlerInfo.Name, "", aResult)
	}
}

// 调用Go查询代理。
func callCrawlerByGolang(crawlerInfo *_db.CrawlerInfoPo, seqNo, carrierCode string, language _types.LangId, trackingNo string) (*agentResult, error) {
	url := crawlerInfo.Url
	if strings.Contains(url, "?") {
		url = url + "&nums=" + trackingNo
	} else {
		url = url + "?nums=" + trackingNo
	}

	log.Printf("[DEBUG] Crawler by golang processing {seq-no: %s, carrier-code: %s, tracking-no: %s} from %s\n", seqNo, carrierCode, trackingNo, url)

	// 固定使用GET方式调用Go查询代理。
	result := agentResult{StartTime: time.Now()}
	if rsp, err := http.Get(url); err != nil {
		// 查询代理不可用。
		return &result, fmt.Errorf("cannot call crawler by golang {crawler-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
			crawlerInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
	} else {
		buf := strings.Builder{}
		if _, err := io.Copy(&buf, rsp.Body); err != nil {
			return &result, fmt.Errorf("cannot read response from crawler by golang {crawler-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
				crawlerInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
		} else {
			result.EndTime = time.Now()
			result.Result = buf.String()
			return &result, nil
		}
	}
}

// 调用Python查询代理。
func callCrawlerByPython(crawlerInfo *_db.CrawlerInfoPo, seqNo, carrierCode string, language _types.LangId, trackingNo string) (*agentResult, error) {
	url := crawlerInfo.Url + "/fetchTrackInfoList"

	data := map[string]interface{}{
		"infoId":            strconv.FormatInt(crawlerInfo.Id, 10),
		"reqUrl":            crawlerInfo.TargetUrl,
		"reqMethod":         crawlerInfo.ReqHttpMethod,
		"reqVerify":         _utils.AsInt(crawlerInfo.Verify, 0),
		"reqJson":           _utils.AsInt(crawlerInfo.Json, 0),
		"reqProxy ":         crawlerInfo.ReqProxy,
		"reqTimeout":        crawlerInfo.ReqTimeout,
		"siteEncrypt":       _utils.AsInt(crawlerInfo.SiteEncrypt, 0),
		"siteCrawlingName":  crawlerInfo.SiteCrawlingName,
		"siteAnalyzedName":  crawlerInfo.SiteAnalyzedName,
		"trackingFieldType": crawlerInfo.TrackingFieldType,
		"trackingFieldName": crawlerInfo.TrackingFieldName,
		"reqHeaders":        crawlerInfo.ReqHttpHeaders,
		"reqData":           crawlerInfo.ReqHttpBody,
		"trackingNo":        trackingNo,
	}
	var dataJson string
	if v, err := json.Marshal(data); err != nil {
		return nil, fmt.Errorf("cannot convert crawler params to json, cause=%w", err)
	} else {
		dataJson = string(v)
	}

	log.Printf("[DEBUG] Crawler by python processing {seq-no: %s, carrier-code: %s, tracking-no: %s} from %s [data=%s]\n", seqNo, carrierCode, trackingNo, url, dataJson)

	// 固定使用POST方式调用Python查询代理。
	result := agentResult{StartTime: time.Now()}

	if rsp, err := http.Post(url, "application/json", strings.NewReader(dataJson)); err != nil {
		// 查询代理不可用。
		return &result, fmt.Errorf("cannot call crawler by golang {crawler-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
			crawlerInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
	} else {
		buf := strings.Builder{}
		if _, err := io.Copy(&buf, rsp.Body); err != nil {
			return &result, fmt.Errorf("cannot read response from crawler by golang {crawler-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
				crawlerInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
		} else {
			result.EndTime = time.Now()
			result.Result = strings.ReplaceAll(strings.ReplaceAll(buf.String(), "'", "\""), "None", "\"\"") // Python查询代理返回的json格式字符串不合规，需要兼容。
			return &result, nil
		}
	}
}

func updateCache(key string, agentSrc _types.TrackingResultSrc, agentName, agentErr string, result *agentResult) {
	if err := _cache.SetAndExpire(key, map[string]interface{}{"status": 1, "agentSrc": int(agentSrc), "agentName": agentName, "agentErr": agentErr, "agentStartTime": _utils.AsString(result.StartTime), "agentEndTime": _utils.AsString(result.EndTime), "agentResult": result.Result}, 10*time.Second); err != nil {
		panic(err)
	}
}
