// 该模块定义了`tracking_log`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"errors"
)

type CarrierPo struct {
	Id        int64
	CountryId int
}

const (
	selectCarrierInfoByCarrierCode string = `select id, country_id from carrier_info where carrier_code = ? and status = 1`
)

func QueryCarrierByCode(carrierCode string) (*CarrierPo, error) {
	// TODO: 使用缓存。
	result := CarrierPo{}
	if err := db.QueryRow(selectCarrierInfoByCarrierCode, carrierCode).Scan(&result.Id, &result.CountryId); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return &result, nil
	}
}
