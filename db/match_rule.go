// 该模块定义了`match_rule`对象的数据库访问方法。
// @Author: Haart
// @Created: 2021-10-27
package db

import (
	"database/sql"
	"errors"
	"regexp"
	"time"
)

// 匹配规则。
type MatchRulePo struct {
	Id         int64          // 规则ID。
	TargetType string         // 目标类型。
	Content    string         // 规则匹配内容。
	Code       string         // 规则对应的目标状态代码。
	rp1        *regexp.Regexp // 用于匹配的正则表达式。
}

// 判断此规则是否匹配目标内容。
// detail 目标内容。
func (m *MatchRulePo) Match(detail string) bool {
	return m.rp1.MatchString(detail)
}

const (
	selectMatchRuleByCarrierCode = `select ter.id, ted.target_type, ter.content, tes1.name_en from tracking_event_rule ter
	join tracking_event_rule_detail ted on ted.event_rule_id = ter.id
	join carrier_info ci on ci.id = ted.carrier_id
	join tracking_event_info tei on tei.id = ter.event_id
	join tracking_event_status tes1 on tes1.id  = tei.event_status_id
	join tracking_event_status tes2 on tes2.id = tes1.parent_id
	where ci.carrier_code = ?
	and ci.status = 1
	and ted.status = 1
	and ter.status = 1
	and tes1.status = 1
	and tes2.status = 1
	and tei.start_time <= ?
	and tei.end_time >= ?
	order by ter.id
	`
)

// 根据运输商号码和时间从数据库中查询有效的匹配规则。
// 如果不存在符合条件的记录则返回空切片。
func QueryMatchRuleByCarrierCode(carrierCode string, datePoint time.Time) ([]*MatchRulePo, error) {
	result := make([]*MatchRulePo, 0)
	if rows, err := db.Query(selectMatchRuleByCarrierCode, carrierCode, datePoint, datePoint); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return result, nil
		} else {
			return result, err
		}
	} else {
		defer rows.Close()

		for rows.Next() {
			matchRule := MatchRulePo{}
			if err := rows.Scan(&matchRule.Id, &matchRule.TargetType, &matchRule.Content, &matchRule.Code); err != nil {
				continue
			}
			if matchRule.Content != "" {
				matchRule.rp1, _ = regexp.Compile(matchRule.Content)
			}
			result = append(result, &matchRule)
		}

		return result, nil
	}
}
