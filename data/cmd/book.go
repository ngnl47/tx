package cmd

// @genReq:"领取图鉴奖励"
// @linkResp:"SyncRewardResp"
type Book_ClaimPointReward struct {
}

// @genReq:"领取图鉴积分"
// @linkResp:"SyncResp"
type Book_ClaimBookPoint struct {
	HeroId int32
}
