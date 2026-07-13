package data

// @genData:`value:"true"`
// @comment:`value:"派遣寻宝功能数据"`
type GDDispatch struct {
	TaskLevel        int32             `comment:"当前派遣任务列表使用的派遣等级"`
	LastReplenishDay int64             `comment:"上次补充任务时的当日零点时间戳，用于跨天检测；0表示未补充过"`
	Tasks            []*GDDispatchTask `comment:"派遣任务列表"`
}

// @genData:`value:"true"`
// @comment:`value:"单条派遣任务；Id 为 seq；固定派遣两个武将，品质需符合 HeroColor；HeroIds 为空且 StartTime==0 表示未派遣"`
type GDDispatchTask struct {
	Id         int64   `comment:"任务唯一id"`
	TaskConfId int32   `comment:"品质id DispatchTaskQualityConf.Id"`
	HeroIds    []int32 `comment:"派遣的武将id列表，固定2个，空表示未派遣"`
	StartTime  int64   `comment:"派遣开始时间 unix秒，0表示未开始"`
	TaskKind   int32   `comment:"任务类型：0普通任务 1升级追加任务"`
	CreateDay  int64   `comment:"任务生成日零点时间戳，用于跨天删除判断"`
}

// @genData:`value:"true"`
// @comment:`value:"派遣任务项（查询返回）；Id 即 seq；每任务两武将；客户端按 taskConfId 查配置取 TaskTime/Icon 等"`
type GDDispatchTaskItem struct {
	Id         int64   `comment:"任务唯一id"`
	TaskConfId int32   `comment:"任务配置id"`
	HeroIds    []int32 `comment:"派遣武将id列表，固定2个"`
	StartTime  int64   `comment:"开始时间"`
	TaskKind   int32   `comment:"任务类型：0普通 1升级追加，用于客户端区分展示"`
}
