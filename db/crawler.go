// 该模块定义了`crawler_info`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"errors"
	"time"
)

type CrawlerInfoPo struct {
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
	selectCrawlerInfoByCarrierCode = `select tci.id,
	tci.name, tci.req_url, tci.type, coalesce(tcp.req_url, ''), coalesce(tcp.req_method, ''), coalesce(tcp.req_headers, ''), coalesce(tcp.req_data, ''), coalesce(tcp.req_verify, 0), coalesce(tcp.req_json, 0), coalesce(tcp.req_proxy, ''),
	coalesce(tcp.req_timeout, 0), coalesce(tcp.site_encrypt, 0), coalesce(tcp.tracking_field_name, ''), coalesce(tcp.tracking_field_type, 0), coalesce(tcp.site_crawling_name, ''), coalesce(tcp.site_analyzed_name, '')
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
	order by tci.priority limit 1
	`

	updateCrawlerHeartBeatNo = `update tracking_crawler_info set heart_beat_no = ? where id = ? and result_status <> 0`
)

func QueryCrawlerInfoByCarrierCode(carrierCode string, datePoint time.Time) *CrawlerInfoPo {
	result := CrawlerInfoPo{}
	if err := db.QueryRow(selectCrawlerInfoByCarrierCode, carrierCode, datePoint, datePoint).Scan(&result.Id, &result.Name, &result.Url, &result.Type, &result.TargetUrl, &result.ReqHttpMethod, &result.ReqHttpHeaders, &result.ReqHttpBody,
		&result.Verify, &result.Json, &result.ReqProxy, &result.ReqTimeout, &result.SiteEncrypt, &result.TrackingFieldName, &result.TrackingFieldType, &result.SiteCrawlingName, &result.SiteAnalyzedName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else {
			panic(err)
		}
	} else {
		return &result
	}
}

// 修改状态为不正常的爬虫的监控运单号。
// 如果爬虫返回结果为成功，那么用这个成功的运单号去更新当前状态为不正常的爬虫的监控运单号。这样可能自动修复监控不正常的问题。
// crawlerId 爬虫ID。
// trackingNo 新的运单号。
// 成功修改的记录数。**注意！！如果新的运单号等于当前运单号，那么实际不会修改任何记录，返回值是0**
func UpgradeHeartBeatNo(crawlerId int64, trackingNo string) int {
	if r, err := db.Exec(updateCrawlerHeartBeatNo, trackingNo, crawlerId); err != nil {
		panic(err)
	} else {
		if c, err := r.RowsAffected(); err != nil {
			panic(err)
		} else {
			return int(c)
		}
	}
}
