package cmd

// @genReq:""
// @linkResp:"SyncResp"
type Hero_HeroUpgradeLevel struct {
	HeroId     int32
	UpgradeNum int32
}

// @genReq:""
// @linkResp:"SyncResp"
type Hero_HeroUpgradeStar struct {
	HeroId int32
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"武将碎片合成"
type Hero_Synthetic struct {
	ItemId int32 `comment:"物品编号"`
}
