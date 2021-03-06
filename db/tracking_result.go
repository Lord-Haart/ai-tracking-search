// 该模块定义了`tracking_result`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_types "com.cne/ai-tracking-search/types"
)

// 保存在数据库中的跟踪结果。
type TrackingResultPo struct {
	CarrierCode string        // 运输商编号。
	Language    _types.LangId // 需要爬取的语言。
	TrackingNo  string        // 运单号。
	UpdateTime  time.Time     // 最新的业务更新时间。
	EventsJson  string        // 事件JSON，也就是查询代理返回的有效结果。
	Done        bool          // 是否已妥投。
}

const (
	selectTrackingResultByTrackingNo string = `select tr.events_json, tr.update_time, coalesce(tr.tracking_status, -1) = 4 from tracking_result tr
	inner join carrier_info ci on ci.id = tr.carrier_id
	where ci.status = 1
	  and tr.status = 1
	  and tr.v2 = 1
	  and ci.carrier_code = ?
	  and tr.tracking_no = ?
	  and tr.language = ?
	  and tr.events_json <> ''
	  order by tr.update_time
	  limit 1
	`

	existsByTrackingNoAndMd5 string = `select exists(select 1 from tracking_result tr where carrier_id = ? and language = ? and tracking_no = ? and md5 = ?)`

	insertTrackingResult string = `insert into tracking_result (carrier_id, language, tracking_no, events_json, md5, status, create_time, update_time, tracking_status, v2)
	values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
)

/*
ALTER TABLE `aitrack`.`tracking_result`
ADD COLUMN `v2` tinyint(4) NOT NULL DEFAULT 0 COMMENT '是否v2版本的记录' AFTER `tracking_status`;
*/

// 根据运输商号码，运单号和查询语言从数据库中查询已存在的爬取结果。
// 如果不存在符合条件的记录则返回nil。
func QueryTrackingResultByTrackingNo(carrierCode string, language _types.LangId, trackingNo string) *TrackingResultPo {
	result := TrackingResultPo{CarrierCode: carrierCode, TrackingNo: trackingNo, Language: language}
	if err := db.QueryRow(selectTrackingResultByTrackingNo, carrierCode, trackingNo, language).Scan(&result.EventsJson, &result.UpdateTime, &result.Done); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else {
			panic(err)
		}
	} else {
		return &result
	}
}

func SaveTrackingResult(carrierId int64, language _types.LangId, trackingNo, eventsJson string, datePoint time.Time, done bool) int64 {
	eventsJsonMd5 := fmt.Sprintf("%x", md5.Sum([]byte(eventsJson)))
	var trackingStatus int
	if done {
		trackingStatus = 4 // 已投递。
	} else {
		trackingStatus = 1 // 在途。
	}
	exists := false
	if err := db.QueryRow(existsByTrackingNoAndMd5, carrierId, language, trackingNo, eventsJsonMd5).Scan(&exists); err != nil {
		panic(err)
	}
	if exists {
		// 如果已存在同样的记录，那么放弃保存。
		return -1
	}

	if result, err := db.Exec(insertTrackingResult, carrierId, language, trackingNo, eventsJson, eventsJsonMd5, 1 /*status*/, datePoint, datePoint, trackingStatus, 1 /*v2*/); err != nil {
		panic(err)
	} else {
		if lastRowId, err := result.LastInsertId(); err != nil {
			panic(err)
		} else {
			return lastRowId
		}
	}
}
