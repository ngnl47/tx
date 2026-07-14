package cmd

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取商品购买数量"
type Card_ClaimReward struct {
	CardId   int32 `comment:"商店编号"`
	ShopType int32 `comment:"商店类型"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取商品月卡奖励"
type Card_ClaimCardReward struct {
	CardId int32 `comment:"卡编号"`
}
