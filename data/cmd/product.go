package cmd

// @genReq:"领取产出奖励"
// @linkResp:"SyncRewardResp"
type Product_GetReward struct {
	Type int32
}

// @genReq:"放置武将"
// @linkResp:"SyncResp"
type Product_PlaceHero struct {
	Type    int32
	HeroIds []int32
}

// @genReq:"放置武将"
// @linkResp:"SyncResp"
type Product_UpgradeLevel struct {
	Type int32
}
