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
	Id              int64
	Code            string
	NameCn          string
	NameEn          string
	CarrierType     _types.CarrierType
	CountryId       int
	WebSiteUrl      sql.NullString
	Tel             sql.NullString
	Email           sql.NullString
	Description     sql.NullString
	ServiceAvaible  bool
	LogoUrl         sql.NullString
	LogoFilename    sql.NullString
	TrackingNoRules []*TrackingNoRulePo
}

type TrackingNoRulePo struct {
	Id   int64
	Name string
	Code string
}

const (
	selectCarrierInfoByCarrierCode string = `select id, country_id from carrier_info where carrier_code = ? and status = 1`
	selectAllCarrierInfo           string = `select distinct ci.id, ci.carrier_code, ci.name_cn, ci.name_en, ci.carrier_type, ci.country_id, ci.website_url, ci.tel, ci.email, ci.description, ci.service_status,
	sba.real_path, sba.file_name,
	tnr.id, tnr.name, tnrd.code
	from carrier_info ci
	left join tracking_no_rule tnr on tnr.carrier_id = ci.id and tnr.status = 1
	left join tracking_no_rule_detail tnrd on tnrd.rule_id = tnr.id and tnrd.status = 1
	left join sys_biz_attachment sba on sba.ext_id = ci.id and sba.ext_type = 1 and sba.status = 1
	where ci.status = 1 and ci.carrier_code is not null
	order by ci.id, tnr.id`
)

func QueryCarrierByCode(carrierCode string) *CarrierPo {
	// TODO: 使用缓存。
	result := CarrierPo{}
	if err := db.QueryRow(selectCarrierInfoByCarrierCode, carrierCode).Scan(&result.Id, &result.CountryId); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else {
			panic(err)
		}
	} else {
		return &result
	}
}

func QueryAllCarrier() []*CarrierPo {
	// TODO: 使用缓存。
	result := make([]*CarrierPo, 0)
	if rows, err := db.Query(selectAllCarrierInfo); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result
		} else {
			panic(err)
		}
	} else {
		carrierPo := (*CarrierPo)(nil)
		for rows.Next() {
			var carrierId int64
			var carrierCode, nameCn, nameEn string
			var carrierType _types.CarrierType
			var countryId int
			var webSiteUrl, tel, email, description sql.NullString
			var serviceAvaible bool
			var logoUrl, logoFilename sql.NullString
			var ruleId sql.NullInt64
			var ruleName sql.NullString
			var ruleCode sql.NullString
			if err := rows.Scan(&carrierId, &carrierCode, &nameCn, &nameEn, &carrierType, &countryId, &webSiteUrl, &tel, &email, &description, &serviceAvaible, &logoUrl, &logoFilename, &ruleId, &ruleName, &ruleCode); err != nil {
				panic(err)
			} else {
				if carrierPo == nil || carrierPo.Id != carrierId {
					if carrierPo != nil {
						result = append(result, carrierPo)
					}
					carrierPo = new(CarrierPo)
					carrierPo.Id = carrierId
					carrierPo.Code = carrierCode
					carrierPo.NameCn = nameCn
					carrierPo.NameEn = nameEn
					carrierPo.CarrierType = carrierType
					carrierPo.CountryId = countryId
					carrierPo.WebSiteUrl = webSiteUrl
					carrierPo.Tel = tel
					carrierPo.Email = email
					carrierPo.Description = description
					carrierPo.ServiceAvaible = serviceAvaible
					carrierPo.LogoUrl = logoUrl
					carrierPo.LogoFilename = logoFilename
					carrierPo.TrackingNoRules = make([]*TrackingNoRulePo, 0)
				}

				if ruleId.Valid {
					carrierPo.TrackingNoRules = append(carrierPo.TrackingNoRules, &TrackingNoRulePo{Id: ruleId.Int64, Name: ruleName.String, Code: ruleCode.String})
				}
			}
		}
		if carrierPo != nil {
			result = append(result, carrierPo)
		}

		return result
	}
}
