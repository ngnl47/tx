package data

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"每日任务"`
type GDDailyTask struct {
	WeekPoint   int32           `comment:"每周任务点数"`
	WeekTime    int64           `comment:"每周任务时间"`
	DailyPoint  int32           `comment:"每日任务点数"`
	DailyTime   int64           `comment:"每日任务时间"`
	Complete    map[int32]int32 `comment:"每日任务状态"`
	DailyReward map[int32]bool  `comment:"日奖励领取状态"`
	WeekReward  map[int32]bool  `comment:"周奖励领取状态"`
	MonthPoint  int32           `comment:"每月任务点数"`
	MonthTime   int64           `comment:"每月任务时间"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"赛季每期任务"`
type GDSPhaseTask struct {
	PhasePoint    int32            `comment:"每期战令积分"`
	PhaseTime     int64            `comment:"每期任务时间"`
	DailyTime     int64            `comment:"每日任务时间"`
	DailyComplete map[int32]int32  `comment:"每日任务状态-1已完成"`
	PhaseComplete map[int32]int32  `comment:"每期任务状态-1已完成"`
	DailyReward   map[int32]int32  `comment:"每日奖积分取状态0未领取,1可领取,2继续领取,3已领取"`
	PhaseReward   map[int32]int32  `comment:"每期奖积分取状态0未领取,1可领取,2继续领取,3已领取"`
	PhaseRecord   map[string]int64 `comment:"期数记录" ignoreClient:"true"`
	PhaseOpen     int64            `comment:"开启时间0为已开启"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"成就"`
type GDAchievement struct {
	CompleteValue int32 `comment:"完成值"`
	LastClaimId   int32 `comment:"最后一个领取的值"`
}
