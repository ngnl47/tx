package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"SyncResp"
type Lord_UpgradeTalentLevel struct {
	UpType data.LordType `comment:"升级类型"`
}

// @genReq:""
// @linkResp:"SyncResp"
type Lord_UpgradeLevel struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
type Lord_ConfirmLevel struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
type Lord_AccelerateConfirmLevel struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
type Lord_AccelerateWithTicket struct {
	Count int32 `comment:"使用数量"`
}

// @genReq:"提交升级天赋材料"
// @linkResp:"SyncResp"
type Lord_CommitTalentMaterial struct {
	ItemId int32 `comment:"物品编号"`
}
