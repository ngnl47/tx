package cmd

// @genReq:"开宝箱"
// @linkResp:"SyncRewardResp"
type Box_Open struct {
	BoxId int32
	Count int32
}

// @genReq:"领取宝箱积分奖励"
// @linkResp:"SyncRewardResp"
type Box_GetScoreReward struct {
}

// @genReq:"一键领取宝箱积分奖励"
// @linkResp:"SyncRewardResp"
type Box_GetBatchScoreReward struct {
}
