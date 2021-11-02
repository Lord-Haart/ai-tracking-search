// 该模块定义了`tracking_log`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"time"
)

const (
	insertTracking string = `insert into tracking(carrier_id, language, tracking_no, delivery_time, destination, collector_type, collector_real_name, create_time, update_time, status)
	values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	insertTrackingDetail string = ``
)

func SaveTrackingToDb(carrierId int, language int, trackingNo string, deliveryTime time.Time, destination string, collectorType int, collectorRealName string, datePoint time.Time) (int64, error) {
	if result, err := db.Exec(insertTracking, carrierId, language, trackingNo, deliveryTime, destination, collectorType, collectorRealName, datePoint, datePoint, 1 /*status*/); err != nil {
		return -1, err
	} else {
		lastRowId, _ := result.LastInsertId()
		return lastRowId, nil
	}
}
