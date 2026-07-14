package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Car_GetRoleCarResp"
// @comment:"获取玩家发车信息"
type Car_GetRoleCar struct {
}

// @genResp:""
type Car_GetRoleCarResp struct {
	SyncResp
	RoleCar *data.GDRoleCar `comment:"玩家发车信息"`
}

// @genReq:""
// @linkResp:"Car_RefreshResp"
// @comment:"刷新玩家车辆信息"
type Car_Refresh struct {
	CarId string `comment:"车辆编号"`
}

// @genResp:""
type Car_RefreshResp struct {
	Car *data.GDCar `comment:"玩家车辆信息"`
	SyncResp
}

// @genReq:""
// @linkResp:"Car_SendResp"
// @comment:"发车"
type Car_Send struct {
	CarId    string          `comment:"车辆编号"`
	HelperId int64           `comment:"助战者ID"`
	Heros    map[int32]int32 `comment:"英雄列表 key是位置(0开始), value是英雄id"`
}

// @genResp:""
type Car_SendResp struct {
	SyncResp
	RoleCar *data.GDRoleCar `comment:"玩家发车信息"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"发布协助阵容"
type Car_SendHelpTeam struct {
}

// @genReq:""
// @linkResp:"Car_GetLegionHelpListResp"
// @comment:"获取军团协助列表"
type Car_GetLegionHelpList struct {
}

// @genResp:""
type Car_GetLegionHelpListResp struct {
	List []*data.GDHelperRoleInfo `comment:"协助列表"`
}

// @genReq:""
// @linkResp:"Car_ClaimResp"
// @comment:"领取奖励"
type Car_Claim struct {
	CarId string `comment:"车辆编号"`
}

// @genResp:""
type Car_ClaimResp struct {
	RoleCar *data.GDRoleCar `comment:"玩家发车信息"`
	Points  int32           `comment:"积分"`
	SyncRewardResp
}

// @genReq:""
// @linkResp:"Car_DetailResp"
// @comment:"车辆详情"
type Car_Detail struct {
	CarId   string `comment:"车辆编号"`
	WithLog bool   `comment:"是否获取车辆历史记录"`
	Chat    bool   `comment:"从聊天记录获取"`
}

// @genResp:""
type Car_DetailResp struct {
	Car     *data.GDCar      `comment:"玩家车辆信息"`
	Logs    []*data.GDCarLog `comment:"车辆历史记录"`
	RoleCar *data.GDRoleCar  `comment:"我的发车信息"`
}

// @genReq:""
// @linkResp:"Car_RecommendListResp"
// @comment:"推荐车辆列表"
type Car_RecommendList struct {
}

// @genResp:""
type Car_RecommendListResp struct {
	RoleCar *data.GDRoleCar `comment:"我的发车信息"`
}

// @genReq:""
// @linkResp:"Car_RaidResp"
// @comment:"掠夺"
type Car_Raid struct {
	CarId string `comment:"车辆编号"`
}

// @genResp:""
type Car_RaidResp struct {
	SyncRewardResp
	BattleData *data.BattleData `comment:"战斗数据"`
	RoleCar    *data.GDRoleCar  `comment:"我的发车信息"`
	Points     int32            `comment:"积分"`
	Multi      bool             `comment:"翻倍"`
}

// @genResp:""
type Car_SyncNotify struct {
	RoleCar *data.GDRoleCar `comment:"我的发车信息"`
}

// @genReq:""
// @linkResp:"Car_GetMemberHelpingCntResp"
// @comment:"获取军团成员正在助战的车辆数"
type Car_GetMemberHelpingCnt struct {
}

// @genResp:""
type Car_GetMemberHelpingCntResp struct {
	MemberHelpingCntMap map[int64]int32 `comment:"成员正在助战的车辆数"`
}

// @genReq:""
// @linkResp:"Car_LogsResp"
// @comment:"车辆历史记录"
type Car_Logs struct {
	LogType data.EMCarLogType `comment:"记录类型"`
	Start   int               `comment:"开始"`
	End     int               `comment:"结束"`
}

// @genResp:""
type Car_LogsResp struct {
	Logs  []*data.GDCarLog `comment:"车辆历史列表"`
	Total int64            `comment:"总条数"`
}

// @genReq:""
// @linkResp:"Car_WantedResp"
// @comment:"通缉"
type Car_Wanted struct {
	RoleId int64 `comment:"角色ID"`
}

// @genResp:""
type Car_WantedResp struct {
	RoleCar *data.GDRoleCar `comment:"玩家发车信息"`
}

// @genReq:""
// @linkResp:"Car_GetMemberRankResp"
// @comment:"获取军团贡献排名"
type Car_GetMemberRank struct{}

// @genResp:""
type Car_GetMemberRankResp struct {
	List     []*data.GDRankMember `comment:"排名列表"`
	MyRank   *data.GDRankMember   `comment:"自己的排名"`
	SeaPoint int64                `comment:"航海积分"`
}

// @genReq:""
// @linkResp:"Car_SwitchServerShowResp"
// @comment:"切换是否刷本服船只"
type Car_SwitchServerShow struct {
}

// @genResp:""
type Car_SwitchServerShowResp struct {
	RoleCar *data.GDRoleCar `comment:"我的发车信息"`
}
