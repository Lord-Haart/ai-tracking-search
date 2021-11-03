// 该模块定义了`tracking_log`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"errors"

	_types "com.cne/ai-tracking-search/types"
)

type CarrierPo struct {
	Id             int64              `json:"id"`
	Code           string             `json:"code"`
	NameCn         string             `json:"nameCn"`
	NameEn         string             `json:"nameEn"`
	CarrierType    _types.CarrierType `json:"carrierType"`
	CountryId      int                `json:"countryId"`
	WebSiteUrl     string             `json:"webSiteUrl"`
	Tel            string             `json:"tel"`
	Email          string             `json:"email"`
	Description    string             `json:"description"`
	ServiceAvaible bool               `json:"serviceAvaiable"`
}

const (
	selectCarrierInfoByCarrierCode string = `select id, country_id from carrier_info where carrier_code = ? and status = 1`
	selectAllCarrierInfo           string = `select id, carrier_code, name_cn, name_en, carrier_type, country_id, website_url, tel, email, description, service_status 
	from carrier_info
	where status = 1 and service_status = 1
	order by id`
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

func QueryAllCarrier() ([]*CarrierPo, error) {
	// TODO: 使用缓存。
	result := make([]*CarrierPo, 0)
	if rows, err := db.Query(selectAllCarrierInfo); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, nil
		} else {
			return result, err
		}
	} else {
		for rows.Next() {
			carrier := CarrierPo{}
			var webSiteUrl, tel, email, description sql.NullString
			if err := rows.Scan(&carrier.Id, &carrier.Code, &carrier.NameCn, &carrier.NameEn, &carrier.CarrierType, &carrier.CountryId, &webSiteUrl, &tel, &email, &description, &carrier.ServiceAvaible); err != nil {
				return result, err
			} else {
				carrier.WebSiteUrl = webSiteUrl.String
				carrier.Tel = tel.String
				carrier.Email = email.String
				carrier.Description = description.String
				result = append(result, &carrier)
			}
		}

		return result, nil
	}
}
