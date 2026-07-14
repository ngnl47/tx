package cmd

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取达标奖励"
type DailyComsume_GetAchieveRwd struct {
	Id int32 `comment:"达标奖励 id"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取勋章奖励"
type DailyComsume_GetExchangeRwd struct {
	Id int32 `comment:"勋章奖励 id"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取达标免费奖励"
type DailyComsume_GetFreeRwd struct {
	ActId int32 `comment:"活动id"`
}
