package cmd

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取兑换码奖励"
type System_ClaimCdkReward struct {
	Key          string `comment:"兑换码"`
	PlatformType string `comment:"平台类型"`
}
