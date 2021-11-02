// 该模块定义了`tracking_log`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"time"
)

const (
	insertTrackingLog string = `insert into tracking_log (carrier_id, tracking_no, match_type, country_id, timing, host, result_status, statistics_date, collector_type, status, 
		create_time, creator, update_time, modifier, request_time, crawler_req_time, crawler_resp_time, crawler_resp_body, result_note)
	values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
)

func SaveTrackingLogToDb(carrierId int64, trackingNo string, matchType int, countryId int, timing int, host string, resultStatus int, statisticsDate time.Time, collectorType int,
	datePoint time.Time, creator string, requestTime, crawlerStartTime, crawlerEndTime time.Time, crawlerRespBody, resultNote string) (int64, error) {
	if result, err := db.Exec(insertTrackingLog, carrierId, trackingNo, matchType, countryId, timing, host, resultStatus, statisticsDate, collectorType, 1 /*status*/, datePoint, creator, datePoint, creator,
		requestTime, crawlerStartTime, crawlerEndTime, crawlerRespBody, resultNote); err != nil {
		return -1, err
	} else {
		lastRowId, _ := result.LastInsertId()
		return lastRowId, nil
	}
}
