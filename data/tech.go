package data

// @genData:`value:"true"`
// @comment:`value:"主公科技信息"`
type GDTechInfo struct {
	Id               int32 `comment:"科技编号"`
	PendingId        int32 `comment:"升级中的下一编号, 0无升级"`
	PendingEndTime   int64 `comment:"升级完成的时间"`
	PendingStartTime int64 `comment:"升级开始时间"`
	PendingSlot      int32 `comment:"在此槽位中升级,0无升级"`
}

// @genData:`value:"true"`
// @comment:`value:"主公科技"`
type GDTech struct {
	Info        map[int32]*GDTechInfo `comment:"已经拥有主公科技"`
	PendingInfo map[int32]*GDTechInfo `comment:"升级中的科技"`
	SlotCnt     int32                 `comment:"槽位数量"`
}
