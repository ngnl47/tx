package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Shop_GetDailyDealInfoResp"
type Shop_GetDailyDealInfo struct {
}

// @genResp:""
// @comment:""
type Shop_GetDailyDealInfoResp struct {
	BuyCount map[int32]int32
}

// @genReq:""
// @linkResp:"Shop_BuyDailyDealResp"
type Shop_BuyDailyDeal struct {
	Id int32
}

// @genResp:""
// @comment:""
type Shop_BuyDailyDealResp struct {
	SyncRewardResp
	BuyCount map[int32]int32
}

// @genReq:""
// @comment:""
// @linkResp:"Shop_CreateOrderResp"
type Shop_CreateOrder struct {
	Type        data.EMChargeType `comment:"充值类型"`
	GoodsId     int32             `comment:"购买内容编号"`
	ActivityId  int32             `comment:"活动编号"`
	IsQR        bool              `comment:"是否是二维码支付"`
	PlatformExt string            `comment:"平台扩展信息"`
	OsType      string            `comment:"客户端操作系统"`
}

// @genResp:""
// @comment:""
type Shop_CreateOrderResp struct {
	OrderInfo     any
	LoginIsExpire bool
}

// @genResp:""
// @comment:""
type Shop_SuccessNotify struct {
	Type    data.EMChargeType    `comment:"充值类型"`
	Role    *data.GDRole         `comment:"角色状态"`
	Reward  []*data.GDRewardData `comment:"挂机奖励"`
	GoodsId int32                `comment:"购买的商品编号"`
	Channel data.ChargeChannel   `comment:"充值渠道"`
}
