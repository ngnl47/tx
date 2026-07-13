package data

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @redis:`routeType:"role" routeBy:"roleId" idField:"roleId" key:"roleActivity"`
// @mongodb:`routeType:"role" routeBy:"roleId" idField:"roleId" collection:"roleActivities" enableSharding:"true"`
// @customExport:`value:"true"`
// @comment:`value:"玩家活动数据"`
type GDActivity struct {
	RoleId          int64              `comment:"角色编号" mongodb:"index,unique"`
	ActNewbie       *GDActNewbie       `comment:"新服战力&招募活动" redis:"subKey:actNewbie"`
	ActBestPlayer   *GDActBestPlayer   `comment:"最强领主活动" redis:"subKey:actBestPlayer"`
	ActServerBattle *GDActServerBattle `comment:"服务器对决公告" redis:"subKey:mergeServerNotice"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"「群雄并起」活动"`
type GDActNewbie struct {
	// 角色触发活动时间戳；结束时刻 = StartTime + NewbieActConstantConf.NewbieActDuration（秒），不落库
	StartTime int64 `comment:"活动开始时间戳"`
	// 战力提升已领取档位，key 为 NewbiePowerActConf.Id
	PowerClaimed map[int32]bool `comment:"已领取战力档位"`
	// 转盘开启累计次数（批量开 N 计 N）
	RecruitCount int32 `comment:"累计使用转盘次数"`
	// 转盘已领取档位，key 为 NewbieRecruitActConf.Id
	RecruitClaimed map[int32]bool `comment:"已领取招募档位"`
	// 元宝兑换礼包累计次数
	GiftCount int32 `comment:"累计元宝兑换礼包次数"`
	// 活动结束后登录结算是否已执行（档位邮件补发标记写入 + 转盘券回收），整段仅一次
	Recycled bool `comment:"活动结束后登录结算是否已完成"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"最强领主活动"`
type GDActBestPlayer struct {
	Phase         string          `comment:"活动期号，与军团战 GDLWarStateInfo.Phase 字符串一致（如 060102）；勿对子字段建全局 unique，多角色同期为常态"`
	ServerIds     []int64         `comment:"本期对战的服务器编号列表"`
	StartTime     int64           `comment:"活动开始时间戳"`
	DailyScores   map[int32]int64 `comment:"各活动日当日积分，key=活动日序号(1-N)；活动总积分=各日之和"`
	TaskScores    map[int32]int32 `comment:"展示用：各配置行历次「单次折算」所得积分累计，key=BestPlayerTaskConf.Id（与道具消耗/发车每次单独 floor 计分一致，无任务进度业务）"`
	TargetClaimed map[int32]bool  `comment:"已领档位，key=BestPlayerTargetConf.Id"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"服务器对决公告"`
type GDActServerBattle struct {
	Open       bool  `comment:"活动是否开启中"`
	UpdateTime int64 `comment:"上次检查时间戳"`
}
