package cmd

import "evo-data/data"

// --- 军团宝箱 LegionBox ---

// @genReq:""
// @linkResp:"LegionBox_ClaimResp"
// @comment:"领取军团宝箱"
type LegionBox_Claim struct {
	LegionId int64  `comment:"军团ID"`
	TypeCode int32  `comment:"宝箱类型"`
	BoxId    string `comment:"宝箱ID"`
}

// @genResp:""
// @comment:"领取军团宝箱返回"
type LegionBox_ClaimResp struct {
	SyncResp
	List []*data.GDLegionBoxClaimRecord `comment:"领取记录"`
}

// @genReq:""
// @linkResp:"LegionBox_BatchClaimResp"
// @comment:"批量领取军团宝箱"
type LegionBox_BatchClaim struct {
	LegionId int64    `comment:"军团ID"`
	TypeCode int32    `comment:"宝箱类型"`
	BoxIds   []string `comment:"宝箱ID列表"`
}

// @genResp:""
// @comment:"批量领取军团宝箱返回"
type LegionBox_BatchClaimResp struct {
	SyncResp
	RewardList []*data.GDRewardData `comment:"合并奖励"`
}

// @genReq:""
// @linkResp:"LegionBox_QueryByTypeResp"
// @comment:"按类型查询军团宝箱列表"
type LegionBox_QueryByType struct {
	LegionId int64 `comment:"军团ID"`
	TypeCode int32 `comment:"宝箱类型"`
}

// @genResp:""
// @comment:"按类型查询军团宝箱列表返回"
type LegionBox_QueryByTypeResp struct {
	List []*data.GDLegionBoxItem `comment:"宝箱列表"`
}

// @genReq:""
// @linkResp:"LegionBox_GetClaimRecordsResp"
// @comment:"获取宝箱领取记录"
type LegionBox_GetClaimRecords struct {
	LegionId int64  `comment:"军团ID"`
	TypeCode int32  `comment:"宝箱类型"`
	BoxId    string `comment:"宝箱ID"`
}

// @genResp:""
// @comment:"获取宝箱领取记录返回"
type LegionBox_GetClaimRecordsResp struct {
	List []*data.GDLegionBoxClaimRecord `comment:"领取记录"`
}

// @genReq:""
// @linkResp:"LegionBox_AddBoxResp"
// @comment:"新增一个宝箱"
type LegionBox_AddBox struct {
	LegionBoxConfId int32 `comment:"宝箱配置 Id"`
}

// @genResp:""
// @comment:"新增一个宝箱返回"
type LegionBox_AddBoxResp struct {
	BoxId string `comment:"宝箱ID"`
}
