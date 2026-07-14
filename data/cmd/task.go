package cmd

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"领取每日任务积分"
type Task_ClaimDailyPoint struct {
	TaskId int32 `comment:"任务编号"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取每日任务奖励"
type Task_ClaimDailyReward struct {
	RewardId int32 `comment:"奖励编号"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取每周任务奖励"
type Task_ClaimWeekReward struct {
	RewardId int32 `comment:"奖励编号"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取成就任务奖励"
type Task_ClaimAchievement struct {
	AchievementId int32 `comment:"奖励编号"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"检测每日任务状态"
type Task_CheckDailyTask struct{}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"领取赛季通行证每日任务积分"
type Task_ClaimSDailyPoint struct {
	TaskId int32 `comment:"任务编号"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"领取赛季通行证每期任务积分"
type Task_ClaimSPhasePoint struct {
	TaskId int32 `comment:"任务编号"`
}

// @genReq:""
// @linkResp:"Task_ClaimSPhaseAllResp"
// @comment:"领取赛季通行证所有积分和奖励"
type Task_ClaimSPhaseAll struct {
}

// @genResp:""
type Task_ClaimSPhaseAllResp struct {
	SyncRewardResp
	LevelId int32 `comment:"当前等级编号"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取赛季通行证奖励"
type Task_ClaimSeasonPassReward struct {
}

// @genResp:""
type Task_ClaimSeasonPassRewardResp struct {
	SyncRewardResp
	LevelId int32 `comment:"当前编号"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取签到通行证奖励"
type Task_ClaimSignInPassReward struct {
}
