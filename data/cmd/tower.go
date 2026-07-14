package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Tower_StartResp"
// @comment:"开始推塔"
type Tower_Start struct{}

// @genResp:""
type Tower_StartResp struct {
	SyncResp
	BattleData *data.BattleData `comment:"战斗数据"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"推塔成功"
type Tower_Success struct {
	InputCode  string `comment:"输入校验码"`
	OutputCode string `comment:"输出校验码"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"推塔失败"
type Tower_Failure struct {
	InputCode  string `comment:"输入校验码"`
	OutputCode string `comment:"输出校验码"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"购买推塔能量"
type Tower_BuyEnergy struct {
	BuyNum int32 `comment:"购买数量"`
}

// @genReq:""
// @linkResp:"Tower_GetRankResp"
// @comment:"获取推塔排名"
type Tower_GetRank struct{}

// @genResp:""
type Tower_GetRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"领取阶段奖励"
type Tower_ClaimReward struct {
	RewardId int32 `comment:"奖励编号"`
}

// @genReq:""
// @linkResp:"Tower_GetInfoResp"
// @comment:"获取爬塔信息"
type Tower_GetInfo struct{}

// @genResp:""
type Tower_GetInfoResp struct {
	SyncResp
	Friends map[int64]int32 `comment:"好友爬塔层数"`
}
