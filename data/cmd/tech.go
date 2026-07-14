package cmd

// @genReq:""
// @linkResp:"SyncResp"
// @comment:""
type Tech_UpgradeTech struct {
	Id int32 `comment:"科技编号"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"获取科技信息"
type Tech_GetInfo struct {
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"科技完成"
type Tech_ResearchComplete struct {
	Id int32 `comment:"科技编号"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"使用元宝科技加速"
type Tech_AccelerateWithCoin struct {
	Id int32 `comment:"科技编号"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"使用加速卷科技加速"
type Tech_AccelerateWithTicket struct {
	Id    int32 `comment:"科技编号"`
	Count int64 `comment:"使用数量"`
}
