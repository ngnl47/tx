package data

// @genData:`value:"true"`
// @customExport:`value:"true"`
type GDRewardData struct {
	Type   int32   `comment:"奖励类型"`
	ItemId int32   `comment:"奖励编号"`
	Value  int64   `comment:"奖励数量"`
	Ext    float64 `comment:"扩展参数"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
type GDLegionRewardData struct {
	Type   int32   `comment:"奖励类型"`
	ItemId int32   `comment:"奖励编号"`
	Value  int64   `comment:"奖励数量"`
	Ext    float64 `comment:"扩展参数"`
}

// @genData:`value:"true"`
type TGARewardData struct {
	ResourceType int32   `comment:"奖励类型"`
	ItemId       int32   `comment:"奖励编号"`
	Value        int64   `comment:"奖励数量"`
	Ext          float64 `comment:"扩展参数"`
}
