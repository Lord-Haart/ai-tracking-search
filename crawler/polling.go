// 该模块定义了轮询爬虫的方法。
// @Author: Haart
// @Created: 2021-10-27
// TODO: 该模块可以被提取为单独的应用。
package crawler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
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

type crawlerResult struct {
	StartTime time.Time // 调用爬虫的时间。
	EndTime   time.Time // 爬虫返回响应的时间。
	Result    string    // 爬虫返回的内容。
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
)

func init() {
	allPriorities = []_types.Priority{_types.PriorityHighest, _types.PriorityHigh, _types.PriorityLow}
}

// 启动轮询。
func Poll() {
	chs := make([]chan int, 10)
	for {
		// TODO: 并发的轮询协程数应当在配置文件中配置。
		for i := 0; i < 10; i++ {
			ch := make(chan int)
			go func() {
				defer func() {
					_utils.RecoverPanic()

					ch <- 1
				}()
				pollOne()
			}()
			chs[i] = ch
		}

		for i := 0; i < 10; i++ {
			<-chs[i]
			close(chs[i])
		}
	}
}

func pollOne() {
	// 依次从不同的优先级队列中获取任务。
	p, key := nextKey()

	if p != -1 {
		seqNo := key[len(TrackingSearchKeyPrefix)+1:]

		if os, err := _cache.Get(key, "reqTime", "carrierCode", "language", "trackingNo"); err != nil {
			log.Printf("[ERROR] Cannot get tracking-search(key=%s) from cache: %s\n", key, err)
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

			// 查询对应的爬虫和参数。
			crawlerInfo := _db.QueryCrawlerInfoByCarrierCode(carrierCode, reqTime)
			if crawlerInfo == nil {
				log.Printf("[WARN] Cannot find suitable crawler for carrier[%s] at %s\n", carrierCode, reqTime)
				updateCache(key, "", &crawlerResult{})
			} else {
				_cache.Set(key, map[string]interface{}{"status": 0})

				var cResult *crawlerResult
				var cErr error
				if crawlerInfo.Type == ctPython {
					// 调用python爬虫。
					cResult, cErr = callCrawlerByPython(crawlerInfo, seqNo, carrierCode, language, trackingNo)
					if cErr != nil {
						log.Printf("[WARN]: Cannot call python crawler %s\n", cErr)
					}
				} else if crawlerInfo.Type == ctGo {
					// 调用Go爬虫。
					cResult, cErr = callCrawlerByGolang(crawlerInfo, seqNo, carrierCode, language, trackingNo)
					if cErr != nil {
						log.Printf("[WARN]: Cannot call golang crawler %s\n", cErr)
					}
				} else {
					log.Printf("[WARN] Unsupported crawler type: %s\n", crawlerInfo.Type)
				}

				if cResult != nil {
					updateCache(key, crawlerInfo.Name, cResult)
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

// 调用Go爬虫。
func callCrawlerByGolang(crawlerInfo *_db.CrawlerInfoPo, seqNo, carrierCode string, language _types.LangId, trackingNo string) (*crawlerResult, error) {
	url := crawlerInfo.Url
	if strings.Contains(url, "?") {
		url = url + "&nums=" + trackingNo
	} else {
		url = url + "?nums=" + trackingNo
	}

	log.Printf("[DEBUG] Crawler by golang processing {seq-no: %s, carrier-code: %s, tracking-no: %s} from %s\n", seqNo, carrierCode, trackingNo, url)

	// 固定使用GET方式调用Go爬虫。
	result := crawlerResult{StartTime: time.Now()}
	if rsp, err := http.Get(url); err != nil {
		// 爬虫不可用。
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

// 调用Python爬虫。
func callCrawlerByPython(crawlerInfo *_db.CrawlerInfoPo, seqNo, carrierCode string, language _types.LangId, trackingNo string) (*crawlerResult, error) {
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

	// 固定使用POST方式调用Python爬虫。
	result := crawlerResult{StartTime: time.Now()}

	if rsp, err := http.Post(url, "application/json", strings.NewReader(dataJson)); err != nil {
		// 爬虫不可用。
		return &result, fmt.Errorf("cannot call crawler by golang {crawler-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
			crawlerInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
	} else {
		buf := strings.Builder{}
		if _, err := io.Copy(&buf, rsp.Body); err != nil {
			return &result, fmt.Errorf("cannot read response from crawler by golang {crawler-name=%s, carrier-code=%s, language=%s, tracking-no=%s seq-no=%s}",
				crawlerInfo.Name, carrierCode, language.String(), trackingNo, seqNo)
		} else {
			result.EndTime = time.Now()
			result.Result = strings.ReplaceAll(strings.ReplaceAll(buf.String(), "'", "\""), "None", "\"\"") // Python爬虫返回的json格式字符串不合规，需要兼容。
			return &result, nil
		}
	}
}

func updateCache(key string, crawlerName string, result *crawlerResult) {
	_cache.Set(key, map[string]interface{}{"status": 1, "crawlerName": crawlerName, "crawlerStartTime": _utils.FormatTime(result.StartTime), "crawlerEndTime": _utils.FormatTime(result.EndTime), "crawlerResult": result.Result})
}
