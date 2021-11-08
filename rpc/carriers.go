package rpc

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"

	_db "com.cne/ai-tracking-search/db"
	_types "com.cne/ai-tracking-search/types"
)

type Carrier struct {
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
	LogoUrl        string             `json:"logoUrl"`
	LogoFilename   string             `json:"logoFilename"`
}

type carriersReq struct {
	idList []int64 `json:"id-list"`
}

type carriersRsp struct {
	commonRsp
	Data []*Carrier `json:"data"`
}

type matchCarrierReq struct {
	TrackingNoList []string `json:"trackingNo"` // 待匹配的运单号列表。
}

type matchCarrierRsp struct {
	commonRsp
	Data [][]*Carrier `json:"data"` // 匹配结果。
}

// 执行运输商信息查询。
func Carriers(ctx *gin.Context) {
	req := carriersReq{}
	// now := time.Now()

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.AbortWithError(400, fmt.Errorf("illegal request: %w", err))
		return
	}

	ctx.JSON(http.StatusOK, buildCarriersRsp(_db.QueryAllCarrier()))
}

// 尝试匹配运输商。
func MatchCarriers(ctx *gin.Context) {
	req := matchCarrierReq{}

	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.AbortWithError(400, fmt.Errorf("illegal request: %w", err))
		return
	}

	trackingNoList := make([]string, 0)
	matchResults := make([][]*_db.CarrierPo, 0)
	for _, trackingNo := range req.TrackingNoList {
		trackingNoList = append(trackingNoList, strings.TrimSpace(trackingNo))
		matchResults = append(matchResults, make([]*_db.CarrierPo, 0))
	}

	allCarrierPo := _db.QueryAllCarrier()

	// 逐个匹配运输商。
	for _, carrierPo := range allCarrierPo {

		// 逐个匹配运输商关联的运单规则。
		for _, tr := range carrierPo.TrackingNoRules {
			for i, trackingNo := range trackingNoList {
				if mm, err := regexp.Match(tr.Code, []byte(trackingNo)); err != nil {
					panic(err)
				} else if mm {
					matchResults[i] = append(matchResults[i], carrierPo)
				}
			}
		}
	}

	ctx.JSON(http.StatusOK, buildMatchCarriersRsp(matchResults))
}

func buildCarriersRsp(carriers []*_db.CarrierPo) *carriersRsp {
	data := make([]*Carrier, 0, len(carriers))

	for _, carrierPo := range carriers {
		data = append(data, carrierPoToCarrier(carrierPo))
	}

	result := carriersRsp{Data: data}
	result.Status = rSuccess
	result.Message = "success"

	return &result
}

func buildMatchCarriersRsp(matchResults [][]*_db.CarrierPo) *matchCarrierRsp {
	data := make([][]*Carrier, 0, len(matchResults))

	for _, mr := range matchResults {
		item := make([]*Carrier, 0, len(mr))
		for _, carrierPo := range mr {
			item = append(item, carrierPoToCarrier(carrierPo))
		}
		data = append(data, item)
	}

	result := matchCarrierRsp{Data: data}
	result.Status = rSuccess
	result.Message = "success"

	return &result
}

func carrierPoToCarrier(carrierPo *_db.CarrierPo) *Carrier {
	return &Carrier{Code: carrierPo.Code, NameCn: carrierPo.NameCn, NameEn: carrierPo.NameEn, CarrierType: carrierPo.CarrierType, CountryId: carrierPo.CountryId, WebSiteUrl: carrierPo.WebSiteUrl.String, Tel: carrierPo.Tel.String,
		Email: carrierPo.Email.String, Description: carrierPo.Description.String, ServiceAvaible: carrierPo.ServiceAvaible, LogoUrl: carrierPo.LogoUrl.String, LogoFilename: carrierPo.LogoFilename.String}

}
