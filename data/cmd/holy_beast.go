package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"HolyBeast_UpgradeLevelResp"
// @comment:"神机兽升级"
type HolyBeast_UpgradeLevel struct {
}

// @genResp:""
type HolyBeast_UpgradeLevelResp struct {
	SyncResp
	IsCrit   bool  `comment:"本次是否暴击"`
	DeltaExp int64 `comment:"经验值变化"`
}

// @genReq:""
// @linkResp:"HolyBeast_SyncEquipResp"
// @comment:"神机兽装备合成"
type HolyBeast_MergeEquip struct {
	Part data.EHBEquipPart `comment:"神机装备部位"`
}

// @genReq:""
// @linkResp:"HolyBeast_SyncEquipResp"
// @comment:"神机兽装备消耗升级, 8级及之后"
type HolyBeast_ConsumeEquip struct {
	Part data.EHBEquipPart `comment:"神机装备部位"`
	Cost map[int32]int32   `comment:"消耗道具itemId:count"`
}

// @genResp:""
type HolyBeast_SyncEquipResp struct {
	SyncRewardResp `comment:"角色状态"`
	List           []*data.RewardDataConfV2 `comment:"该部位道具列表"`
	CurExp         int32                    `comment:"当前经验(>=8级时用)"`
	NextExp        int32                    `comment:"下一级经验(>=8级时用)"`
	CurBonus       map[int32]float64        `comment:"当前加成"`
	NextBonus      map[int32]float64        `comment:"下一级加成"`
}

// @genReq:""
// @linkResp:"HolyBeast_SyncEquipResp"
// @comment:"神机兽装备升级,8级前"
type HolyBeast_UpEquip struct {
	Part data.EHBEquipPart `comment:"神机装备部位"`
}

// @genReq:""
// @linkResp:"HolyBeast_SyncEquipResp"
// @comment:"神机兽装备替换"
type HolyBeast_ReplaceEquip struct {
	Part data.EHBEquipPart `comment:"神机装备部位"`
}

// @genReq:""
// @linkResp:"HolyBeast_GetEquipResp"
// @comment:"获取神机兽装备配件"
type HolyBeast_GetEquip struct {
	Part data.EHBEquipPart `comment:"神机装备部位"`
}

// @genResp:""
type HolyBeast_GetEquipResp struct {
	List      []*data.RewardDataConfV2 `comment:"该部位道具列表"`
	CurExp    int32                    `comment:"当前经验(>=8级时用)"`
	NextExp   int32                    `comment:"下一级经验(>=8级时用)"`
	CurBonus  map[int32]float64        `comment:"当前加成"`
	NextBonus map[int32]float64        `comment:"下一级加成"`
}

// @genReq:""
// @linkResp:"HolyBeast_GetEquipAllResp"
// @comment:"获取神机兽装备配件"
type HolyBeast_GetEquipAll struct {
}

// @genResp:""
type HolyBeast_GetEquipAllResp struct {
	List []*data.RewardDataConfV2 `comment:"道具列表"`
}
