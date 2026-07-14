package cmd

import "evo-data/data"

// @genReq:"开始打主线关卡"
// @linkResp:"Fight_StartLevelResp"
type Fight_StartLevel struct {
}

// @genResp:""
// @comment:"开始打主线关卡返回"
type Fight_StartLevelResp struct {
	BattleData *data.BattleData
}

// @genReq:"结束打主线关卡"
// @linkResp:"SyncResp"
type Fight_EndLevel struct {
	LevelId    int32
	BattleTime int32
	OutputCode string
	Log        string
}

// @genReq:""
// @linkResp:"Fight_StartDungeonResp"
// @comment:"挑战副本"
type Fight_StartDungeon struct {
	DungeonType int32 `comment:"副本类型"`
}

// @genResp:""
type Fight_StartDungeonResp struct {
	SyncRewardResp
	BattleData *data.BattleData `comment:"battle data"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"扫荡副本"
type Fight_SweepDungeon struct {
	DungeonType int32 `comment:"副本类型"`
}

// @genReq:""
// @linkResp:"Fight_StartTowerResp"
// @comment:"爬塔战斗"
type Fight_StartTower struct {
}

// @genResp:""
// @comment:"爬塔战斗返回"
type Fight_StartTowerResp struct {
	SyncRewardResp
	BattleData *data.BattleData `comment:"战斗数据"`
}

// @genReq:""
// @linkResp:"Fight_StartBossResp"
// @comment:"BOSS战斗"
type Fight_StartBoss struct {
	BossId int32 `comment:"BOSS编号"`
}

// @genResp:""
// @comment:"BOSS战斗返回"
type Fight_StartBossResp struct {
	SyncRewardResp
	BattleData *data.BattleData `comment:"战斗数据"`
	TotalHurt  int64            `comment:"总伤害"`
}

// @genReq:""
// @linkResp:"Fight_StartPVPResp"
// @comment:"PVP战斗"
type Fight_StartPVP struct {
	TargetId int64 `comment:"目标编号"`
}

// @genResp:""
// @comment:"PVP战斗返回"
type Fight_StartPVPResp struct {
	BattleData *data.BattleData `comment:"战斗数据"`
}

// @genReq:""
// @linkResp:"Fight_StartAreaArenaResp"
// @comment:"跨服竞技场战斗"
type Fight_StartAreaArena struct {
	TargetId int64 `comment:"目标角色编号"`
}

// @genResp:""
// @comment:"跨服竞技场战斗返回"
type Fight_StartAreaArenaResp struct {
	SyncRewardResp
	BattleData   *data.BattleData   `comment:"战斗数据"`
	TargetRole   *data.GDSimpleRole `comment:"对方信息"`
	SelfScore    int32              `comment:"自己积分变化"`
	OppoScore    int32              `comment:"对方积分变化"`
	SelfCurScore int32              `comment:"自己当前积分"`
	OppoCurScore int32              `comment:"对方当前积分"`
}
