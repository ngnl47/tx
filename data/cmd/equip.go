package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"装备升级"
type Equipment_UpgradeLevel struct {
	HeroId     int32            `comment:"装备编号"`
	Part       data.EMEquipPart `comment:"装备编号"`
	UpgradeNum int32            `comment:"升级次数"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"装备单个重生"
type Equipment_Rebirth struct {
	HeroId int32            `comment:"槽位"`
	Part   data.EMEquipPart `comment:"装备编号"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"专属装备升级"
type Equipment_UpgradeSpLevel struct {
	HeroId int32 `comment:"英雄编号"`
}
