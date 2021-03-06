// 该模块定义了`tracking_log`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"time"

	_types "com.cne/ai-tracking-search/types"
)

const (
	insertTracking string = `insert into tracking(carrier_id, language, tracking_no, delivery_time, destination, collector_type, collector_real_name, create_time, update_time, status)
	values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	insertTrackingDetail string = `insert into tracking_detail(info_id, date, place, details, state, event_id, event_name, event_rule_match, status, create_time, update_time)
	values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	deleteTracking string = `delete from tracking where carrier_id = ? and language = ? and tracking_no = ?`
)

func SaveTrackingToDb(carrierId int64, language _types.LangId, trackingNo string, deliveryTime time.Time, destination string, collectorType _types.TrackingResultSrc, collectorRealName string, datePoint time.Time, done bool) int64 {
	deliveryTime_ := sql.NullTime{Time: deliveryTime, Valid: done}
	destination_ := sql.NullString{String: destination, Valid: destination != ""}
	if result, err := db.Exec(insertTracking, carrierId, int(language), trackingNo, deliveryTime_, destination_, int(collectorType), collectorRealName, datePoint, datePoint, 1 /*status*/); err != nil {
		panic(err)
	} else {
		if lastRowId, err := result.LastInsertId(); err != nil {
			panic(err)
		} else {
			return lastRowId
		}
	}
}

func SaveTrackingDetailToDb(infoId int64, date time.Time, place string, details string, state int, datePoint time.Time) int64 {
	if result, err := db.Exec(insertTrackingDetail, infoId, date, place, details, state, sql.NullInt64{}, sql.NullString{}, sql.NullInt16{}, 1 /*status*/, datePoint, datePoint); err != nil {
		panic(err)
	} else {
		if lastRowId, err := result.LastInsertId(); err != nil {
			panic(err)
		} else {
			return lastRowId
		}
	}
}

func DeleteTracking(carrierId int64, language _types.LangId, trackingNo string) int64 {
	if result, err := db.Exec(deleteTracking, carrierId, int(language), trackingNo); err != nil {
		panic(err)
	} else {
		if c, err := result.RowsAffected(); err != nil {
			panic(err)
		} else {
			return c
		}
	}
}
