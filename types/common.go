// 该模块定义了其它模块需要的类型。
// @Author: Haart
// @Created: 2021-10-27
package types

import (
	"encoding/json"
	"fmt"
	"strings"
)

// 运输商类别。
// 注意：外部接口中包含该类型，并且需要以字符串形式传递，所以需要为该类型提供自定义的`MarshalJSON`和`UnsarshalJSON`
type CarrierType int

const (
	CtEMS           CarrierType = 1 // 全球邮政。
	CtUnion         CarrierType = 2 // 全球联合运输商。
	CtInternational CarrierType = 3 // 国际运输商。
	CtCN            CarrierType = 4 // 中国运输商。
	CtAirline       CarrierType = 5 // 航空公司。
)

func (ct *CarrierType) String() string {
	if *ct == CtEMS {
		return "EMS"
	} else if *ct == CtUnion {
		return "UNION"
	} else if *ct == CtInternational {
		return "INTERNATIONAL"
	} else if *ct == CtCN {
		return "CN"
	} else if *ct == CtAirline {
		return "AIRLINE"
	} else {
		return ""
	}
}

// 将字符串解析为LangId
// s 待解析的字符串，会被自动去除首尾空格，然后变为大写。
// 返回解析结果。
func ParseCarrierType(s string) (CarrierType, error) {
	s = strings.ToUpper(strings.TrimSpace(s))

	if s == "EMS" {
		return CtEMS, nil
	} else if s == "UNION" {
		return CtUnion, nil
	} else if s == "INTERNATIONAL" {
		return CtInternational, nil
	} else if s == "CN" {
		return CtCN, nil
	} else if s == "AIRLINE" {
		return CtAirline, nil
	} else {
		return 0, fmt.Errorf("unkown carrier type: %s", s)
	}
}

func (ct *CarrierType) MarshalJSON() ([]byte, error) {
	return json.Marshal(ct.String())
}

func (l *CarrierType) UnmarshalJSON(b []byte) error {
	s := ""
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	} else if ll, err := ParseCarrierType(s); err != nil {
		return err
	} else {
		*l = ll
		return nil
	}
}

// 爬取的语言类型。
// 注意：外部接口中包含该类型，并且需要以字符串形式传递，所以需要为该类型提供自定义的`MarshalJSON`和`UnsarshalJSON`
type LangId int

const (
	LangCN LangId = 1 // 中文
	LangEN LangId = 2 // 英文
)

func (l *LangId) String() string {
	if *l == LangEN {
		return "EN"
	} else if *l == LangCN {
		return "CN"
	} else {
		return ""
	}
}

// 将字符串解析为LangId
// s 待解析的字符串，会被自动去除首尾空格，然后变为大写。
// 返回解析结果。
func ParseLangId(s string) (LangId, error) {
	s = strings.ToUpper(strings.TrimSpace(s))

	if s == "EN" {
		return LangEN, nil
	} else if s == "CN" {
		return LangCN, nil
	} else {
		return 0, fmt.Errorf("unkown lang id: %s", s)
	}
}

func (l *LangId) MarshalJSON() ([]byte, error) {
	return json.Marshal(l.String())
}

func (l *LangId) UnmarshalJSON(b []byte) error {
	s := ""
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	} else if ll, err := ParseLangId(s); err != nil {
		return err
	} else {
		*l = ll
		return nil
	}
}

// 优先级。
type Priority int

const (
	PriorityHighest Priority = 0 // 最高优先级
	PriorityHigh    Priority = 1 // 高优先级
	PriorityLow     Priority = 2 // 低优先级
)

func (p *Priority) String() string {
	if *p == PriorityHighest {
		return "Highest"
	} else if *p == PriorityHigh {
		return "High"
	} else if *p == PriorityLow {
		return "Low"
	} else {
		return ""
	}
}

// 查询代理结果的来源。
type TrackingResultSrc int

const (
	SrcUnknown TrackingResultSrc = -1 // 未知。
	SrcAPI     TrackingResultSrc = 0  // 来自API。
	SrcCrawler TrackingResultSrc = 1  // 来自爬虫。
	SrcDB      TrackingResultSrc = 3  // 来自数据库。
)

func (s *TrackingResultSrc) String() string {
	if *s == SrcDB {
		return "DB"
	} else if *s == SrcAPI {
		return "API"
	} else if *s == SrcCrawler {
		return "Crawler"
	} else {
		return ""
	}
}
