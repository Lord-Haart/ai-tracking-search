// 该模块定义了`api_info`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"errors"
	"time"
)

type ApiInfoPo struct {
	Name string // 查询代理名称。
	Url  string // 访问查询代理的URL。

	ReqHttpType int // 1-GET 2-POST。

	Id int64 // API设置ID。
}

type ApiParamPo struct {
	FieldType   int    // 字段类型。
	FieldName   string // 字段名。
	FieldValue  string // 字段值。
	IsHead      bool   // 是否请求头字段。
	IsBody      bool   // 是否请求体字段。
	NeedCrypt   bool   // 是否需要加密。
	EncryptType int    // 加密算法。
}

const (
	selectApiInfoByCarrierCode = `select ta.id, ta.name, ta.api_url, ta.request_type
from tracking_api ta
join carrier_info ci on ci.id = ta.carrier_id
where ci.carrier_code = ?
  and ci.status = 1
	and ta.status = 1
	and ta.service_status = 1
	and ta.start_time <= ?
	and ta.end_time >= ?
	`

	selectApiParamsByApiId = `select tap.field_type, tap.field_name, tap.field_value, tap.is_head_param, tap.is_body_param, tap.need_encrypt, coalesce(tae.encrypt_type, 0)
	from tracking_api_param tap
	left join tracking_api_encrypt tae on tap.encrypt_id = tae.id and tae.status = 1
	where api_id = ?
	and tap.status = 1
	order by tap.sort
	`
)

func QueryApiInfoByCarrierCode(carrierCode string, datePoint time.Time) *ApiInfoPo {
	result := ApiInfoPo{}
	if err := db.QueryRow(selectApiInfoByCarrierCode, carrierCode, datePoint, datePoint).Scan(&result.Id, &result.Name, &result.Url, &result.ReqHttpType); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else {
			panic(err)
		}
	} else {
		return &result
	}
}

func QueryApiParamsByApiId(apiId int64) []*ApiParamPo {
	result := make([]*ApiParamPo, 0)
	if rows, err := db.Query(selectApiParamsByApiId, apiId); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result
		} else {
			panic(err)
		}
	} else {
		defer rows.Close()

		for rows.Next() {
			apiParam := ApiParamPo{}
			if err := rows.Scan(&apiParam.FieldType, &apiParam.FieldName, &apiParam.FieldValue, &apiParam.IsHead, &apiParam.IsBody, &apiParam.NeedCrypt, &apiParam.EncryptType); err != nil {
				panic(err)
			}
			result = append(result, &apiParam)
		}

		return result
	}
}
