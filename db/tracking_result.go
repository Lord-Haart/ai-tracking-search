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
	EventsJson  string        // 事件JSON，也就是爬虫返回的有效结果。
	Done        bool          // 是否已妥投。
}

const (
	selectTrackingResultByTrackingNo string = `select tr.events_json, tr.update_time, tr.tracking_status = 4 from tracking_result tr
	inner join carrier_info ci on ci.id = tr.carrier_id
	where ci.status = 1
	  and tr.status = 1
	  and tr.v2 = 1
	  and ci.carrier_code = ?
	  and tr.tracking_no = ?
	  and tr.language = ?
	`

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
func QueryTrackingResultByTrackingNo(carrierCode string, language _types.LangId, trackingNo string) (*TrackingResultPo, error) {
	result := TrackingResultPo{CarrierCode: carrierCode, TrackingNo: trackingNo, Language: language}
	if err := db.QueryRow(selectTrackingResultByTrackingNo, carrierCode, trackingNo, language).Scan(&result.EventsJson, &result.UpdateTime, &result.Done); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return &result, nil
	}
}

func SaveTrackingResultToDb(carrierId int64, language _types.LangId, trackingNo, eventsJson string, datePoint time.Time, done bool) (int64, error) {
	// TODO: 同时保存匹配之前的原始报文。
	eventsJsonMd5 := fmt.Sprintf("%x", md5.Sum([]byte(eventsJson)))
	var trackingStatus int
	if done {
		trackingStatus = 4 // 已投递。
	} else {
		trackingStatus = 1 // 在途。
	}
	if result, err := db.Exec(insertTrackingResult, carrierId, language, trackingNo, eventsJson, eventsJsonMd5, 1 /*status*/, datePoint, datePoint, trackingStatus, 1 /*v2*/); err != nil {
		return -1, err
	} else {
		lastRowId, _ := result.LastInsertId()
		return lastRowId, nil
	}
}
