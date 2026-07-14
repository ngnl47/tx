package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"客户端置设自定义数据"
type System_Custom struct {
	Key   string `comment:"自定义键"`
	Value int64  `comment:"自定义值"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"玩家改名"
type System_EditName struct {
	Name string `comment:"新的昵称"`
}

// @genReq:""
// @linkResp:"System_GetDataBundleVerResp"
// @comment:"获取配置版本号"
type System_GetDataBundleVer struct {
	IsAudit bool `comment:"是否审核版本"`
}

// @genResp:""
// @comment:"获取配置版本号返回"
type System_GetDataBundleVerResp struct {
	DataBundleVer string `comment:"版本号"`
}

// @genReq:""
// @linkResp:"System_GetOtherInfoResp"
// @comment:"获取角色信息"
type System_GetOtherInfo struct {
	TargetId   int64   `comment:"角色编号"`
	HeroIdList []int32 `comment:"英雄列表"`
}

// @genResp:""
type System_GetOtherInfoResp struct {
	RoleInfo *data.GDRole `comment:"角色信息"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取挂机奖励"
type System_ClaimStayReward struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取主线奖励"
type System_ClaimLevelReward struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取侧边栏礼包"
type System_GetSideGift struct {
	Status int32 `comment:"步骤"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取侧边栏礼包"
type System_GetDesktopGift struct {
	Status int32 `comment:"步骤"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"奖励补发"
type Role_BackClaimReward struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"跨天奖励补发"
type Role_CrossDayClaim struct {
}

// @genReq:""
// @linkResp:"SyncResp"
type System_UpdateHeadImg struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"整点补发"
type Role_HourClaimReward struct {
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"玩家穿戴装扮"
type Role_LoadDress struct {
	DressType data.EMDressType `comment:"装扮类型"`
	Id        int32            `comment:"装扮编号"`
}

// @genReq:""
// @linkResp:"Role_GetSeasonPassInfoResp"
// @comment:"获取当前赛季通行证档位"
type Role_GetSeasonPassInfo struct {
	Phase string `comment:"期数"`
}

// @genResp:""
type Role_GetSeasonPassInfoResp struct {
	LevelId int32 `comment:"当前编号"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取vip每日免费礼包"
type Role_GetVipDailyGift struct {
	LevelId int32 `comment:"vip等级"`
}
