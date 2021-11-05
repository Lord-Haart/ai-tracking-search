// 该模块定义了`api_info`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"time"
)

type ApiInfoPo struct {
	Name string // 查询代理名称。
	Url  string // 访问查询代理的URL。
	Type string // 查询代理类型。

	Id                int64  // 查询代理ID。
	TargetUrl         string // 目标网页的URL。
	ReqHttpMethod     string // 访问目标网页的HTTP Method
	ReqHttpHeaders    string // 访问目标网页附带的头部。
	ReqHttpBody       string // 访问目标网页附带的数据。
	Verify            bool   // 是否需要验证请求结果。TODO: 以后删除。
	Json              bool   // 是否需要将payload序列化为json。TODO: 以后修改为 requestContentType
	ReqProxy          string // 代理服务器。
	ReqTimeout        int    // 访问目标网页的超时时间。
	SiteEncrypt       int    // 目标站点是否加密 0-不加密，1-需要加密。
	TrackingFieldName string // 附加字段名。
	TrackingFieldType int    // 附加字段类型。
	SiteCrawlingName  string
	SiteAnalyzedName  string
}

const (
	selectApiInfoByCarrierCode = `select tci.id,
	tci.name, tci.req_url, tci.type, tcp.req_url, tcp.req_method, tcp.req_headers, tcp.req_data, tcp.req_verify, tcp.req_json, tcp.req_proxy,
	tcp.req_timeout, tcp.site_encrypt, tcp.tracking_field_name, tcp.tracking_field_type, tcp.site_crawling_name, tcp.site_analyzed_name
from tracking_crawler_info  tci
left join tracking_crawler_param tcp on tcp.info_id = tci.id
join carrier_info ci on ci.id = tci.carrier_id
where ci.carrier_code = ?
  and ci.status = 1
	and tci.status = 1
	and (tcp.status = 1 or tcp.status is null)
	and tci.service_status = 1
	and tci.start_time <= ?
	and tci.end_time >= ?
	`
)

func QueryApiInfoByCarrierCode(carrierCode string, datePoint time.Time) *ApiInfoPo {
	return nil
	// result := ApiInfoPo{}
	// if err := db.QueryRow(selectCrawlerInfoByCarrierCode, carrierCode, datePoint, datePoint).Scan(&result.Id, &result.Name, &result.Url, &result.Type, &result.TargetUrl, &result.ReqHttpMethod, &result.ReqHttpHeaders, &result.ReqHttpBody,
	// 	&result.Verify, &result.Json, &result.ReqProxy, &result.ReqTimeout, &result.SiteEncrypt, &result.TrackingFieldName, &result.TrackingFieldType, &result.SiteCrawlingName, &result.SiteAnalyzedName); err != nil {
	// 	if errors.Is(err, sql.ErrNoRows) {
	// 		return nil
	// 	} else {
	// 		panic(err)
	// 	}
	// } else {
	// 	return &result
	// }
}
