package data

// @genData:`value:"true"`
// @comment:`value:"开服定时任务"`
type GDServerOpenTask struct {
	Platform       string                `comment:"平台"`
	PlatformExt    string                `comment:"平台扩展"`
	ExecuteTime    int64                 `comment:"执行时间戳(必然是未来某天的 00:01:00)"`
	PlayerNum      int64                 `comment:"触发玩家数阈值(0表示纯定时，>0需满足玩家数),任务执行前的校验参数"`
	ExpectServerId int64                 `comment:"预期开启服务器ID(ServerId),任务执行前的校验参数"`
	State          EMServerOpenTaskState `comment:"状态:0已执行,1待执行,2已跳过"`
	Msg            string                `comment:"执行状态描述(记录跳过原因或执行结果)"`
	ActualServerId int64                 `comment:"实际创建的服务器ID(执行后填充)"`
	CreateTime     int64                 `comment:"任务创建时间"`
	UpdateTime     int64                 `comment:"更新时间"`
}

// @genEnum:`value:"true"`
// @comment:`value:"任务执行状态"`
type EMServerOpenTaskState struct {
	Finish  int32 `enum:"0" comment:"已执行"`
	Ready   int32 `enum:"1" comment:"待执行"`
	Skipped int32 `enum:"2" comment:"已跳过"`
}
