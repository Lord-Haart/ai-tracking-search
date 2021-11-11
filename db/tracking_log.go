// 该模块定义了`tracking_log`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"time"

	_types "com.cne/ai-tracking-search/types"
	_utils "com.cne/ai-tracking-search/utils"
)

const (
	insertTrackingLog string = `insert into tracking_log (carrier_id, tracking_no, match_type, country_id, timing, host, result_status, statistics_date, collector_type, status, 
		create_time, creator, update_time, modifier, request_time, crawler_req_time, crawler_resp_time, crawler_resp_body, result_note)
	values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
)

func SaveTrackingLogToDb(carrierId int64, trackingNo string, matchType int, countryId int, timing int, host string, resultStatus int, statisticsDate time.Time, collectorType _types.TrackingResultSrc,
	datePoint time.Time, creator string, requestTime, crawlerStartTime, crawlerEndTime time.Time, crawlerRespBody, resultNote string) int64 {
	crawlerStartTime_ := sql.NullTime{Time: crawlerStartTime, Valid: !_utils.IsZeroTime(crawlerStartTime)}
	crawlerEndTime_ := sql.NullTime{Time: crawlerEndTime, Valid: !_utils.IsZeroTime(crawlerEndTime)}
	collectorType_ := sql.NullInt32{Int32: int32(collectorType), Valid: collectorType != _types.SrcUnknown}
	if result, err := db.Exec(insertTrackingLog, carrierId, trackingNo, matchType, countryId, timing, host, resultStatus, statisticsDate, collectorType_, 1 /*status*/, datePoint, creator, datePoint, creator,
		requestTime, crawlerStartTime_, crawlerEndTime_, crawlerRespBody, resultNote); err != nil {
		panic(err)
	} else {
		if lastRowId, err := result.LastInsertId(); err != nil {
			panic(err)
		} else {
			return lastRowId
		}
	}
}
