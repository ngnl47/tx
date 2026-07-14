package cmd

import "evo-data/data"

// @genResp:""
type War_BFResp struct {
	BF *data.GDBattlefield
}

// @genResp:""
// @comment:"军团战状态变更通知"
type War_StateChangeNotify struct {
	War_BFResp
}

// @genReq:""
// @comment:"进入战场"
type War_EnterBF struct {
}

// @genResp:""
// @comment:"进入战场响应"
type War_EnterBFResp struct {
	War_BFResp
}

// @genResp:""
// @comment:"军团战数据变更通知"
type War_DataChangeNotify struct {
	War_BFResp
}

// @genReq:""
// @comment:"游客进入战场"
type War_GuestEnterBF struct {
}

// @genResp:""
// @comment:"游客进入战场响应"
type War_GuestEnterBFResp struct {
	War_BFResp
	EnterRoleIds []int64 `comment:"进入战场角色编号"`
}

// @genReq:""
// @comment:"离开战场"
type War_LeaveBF struct {
}

// @genResp:""
// @comment:"离开战场响应"
type War_LeaveBFResp struct {
	War_BFResp
	LeaveRoleIds []int64 `comment:"离开战场角色编号"`
}

// @genReq:""
// @comment:"设置阵容"
type War_SetBattleTeam struct {
	BirthBuildId int64           `comment:"出生建筑编号"`
	BattleTeam   map[int32]int32 `comment:"进入战场阵容"`
}

// @genResp:""
// @comment:"设置阵容响应"
type War_SetBattleTeamResp struct {
	War_BFResp
}

// @genReq:""
// @comment:"开始行军"
type War_StartMarch struct {
	TargetBuildId int64 `comment:"目标建筑编号"`
}

// @genResp:""
// @comment:"结束行军响应"
type War_StartMarchResp struct {
	War_BFResp
	MarchIds     []int64 `comment:"行军编号"`
	MarchRoleIds []int64 `comment:"行军角色编号"`
}

// @genResp:""
// @comment:"结束行军响应"
type War_EndMarchNotify struct {
	War_BFResp
	MarchIds     []int64 `comment:"行军编号"`
	MarchRoleIds []int64 `comment:"行军角色编号"`
}

// @genReq:""
// @comment:"行军加速"
type War_SpeedUp struct {
	MarchId int64 `comment:"行军编号"`
}

// @genResp:""
// @comment:"行军加速响应"
type War_SpeedUpResp struct {
	War_BFResp
	MarchIds []int64 `comment:"行军编号"`
}

// @genReq:""
// @comment:"开始攻城"
type War_AttackBuilding struct {
}

// @genResp:""
// @comment:"开始攻城响应"
type War_AttackBuildingResp struct {
	War_BFResp
}

// @genResp:""
// @comment:"结束攻城通知"
type War_EndAttackBuildNotify struct {
	War_BFResp
	BattleIds     []int64 `comment:"战斗编号"`
	AttackRoleIds []int64 `comment:"攻击角色编号"`
}

// @genReq:""
// @comment:"开始战斗"
type War_StartBattle struct {
	TargetId int64 `comment:"目标编号"`
}

// @genResp:""
// @comment:"开始战斗响应"
type War_StartBattleResp struct {
	War_BFResp
}

// @genResp:""
// @comment:"结束战斗通知"
type War_EndBattleNotify struct {
	War_BFResp
	BattleIds []int64          `comment:"战斗编号"`
	WinFlags  []data.EMWinFlag `comment:"是否胜利"`
}

// @genResp:""
// @comment:"建筑改变通知"
type War_BuildingChangeNotify struct {
	War_BFResp
}

// @genResp:""
// @comment:"军团增加积分通知"
type War_LegionPointChangeNotify struct {
	War_BFResp
}

// @genReq:""
// @comment:"自杀"
type War_Suicide struct {
}

// @genResp:""
// @comment:"自杀响应"
type War_SuicideResp struct {
	War_BFResp
	SuicideRoleIds []int64 `comment:"自杀角色编号"`
}

// @genReq:""
// @comment:"使用道具复活"
type War_Resurrect struct {
}

// @genResp:""
// @comment:"复活通知"
type War_ResurrectNotify struct {
	War_BFResp
	ResurrectRoleIds []int64 `comment:"复活角色编号"`
}

// @genReq:""
// @comment:"同步道具"
type War_SyncItem struct {
}

// @genResp:""
// @comment:"同步道具通知"
type War_SyncItemNotify struct {
	ReviveItem int32 `comment:"复活丹"`
}

// @genReq:""
// @comment:"获取战场信息"
type War_GetBFInfo struct {
	IsForce bool          `comment:"是否强制"`
	Pos     *data.GDPoint `comment:"位置"`
}

// @genResp:""
// @comment:"获取战场信息响应"
type War_GetBFInfoResp struct {
	War_BFResp
}

// @genReq:""
// @comment:"获取阵容信息"
type War_GetTeamInfo struct {
	TargetId int64 `comment:"目标编号"`
}

// @genResp:""
// @comment:"获取阵容信息响应"
type War_GetTeamInfoResp struct {
	TeamInfo map[int32]*data.GDBFHero `comment:"阵容信息"`
}

// @genReq:""
// @comment:"获取角色信息"
type War_GetRolesInfo struct {
	TargetIds []int64 `comment:"目标编号"`
}

// @genResp:""
// @comment:"获取角色信息响应"
type War_GetRolesInfoResp struct {
	RolesInfo map[int64]*data.GDBFRoleInfo `comment:"角色信息"`
}

// @genReq:""
// @comment:"获取所有建筑信息"
type War_GetAllBuildings struct {
}

// @genResp:""
// @comment:"获取所有建筑信息响应"
type War_GetAllBuildingsResp struct {
	BuildingsInfo map[int64]*data.GDBuilding `comment:"所有建筑信息"`
}

// @genResp:""
// @comment:"连杀/终结连杀广播"
type War_ConKillNotify struct {
	War_BFResp
}
