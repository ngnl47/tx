package cmd

import "evo-data/data"

// @genReq:"获取匹配信息"
// @linkResp:"Match_GetMatchInfoResp"
type Match_GetMatchInfo struct {
}

// @genResp:""
// @comment:"获取匹配信息返回"
type Match_GetMatchInfoResp struct {
	MatchState     data.MatchState
	StartMatchTime int64  `comment:"开始匹配时间"`
	RoomId         string `comment:"房间id"`
	RoomRpcId      string `comment:"房间rpc id"`
	IsInGuide      bool   `comment:"是否在引导中"`
	RoleList       []*data.LB_PlayerInfo
}

// @genReq:"开始匹配"
// @linkResp:"Match_StartMatchResp"
type Match_StartMatch struct {
}

// @genResp:""
// @comment:"开始匹配返回"
type Match_StartMatchResp struct {
	SyncResp
}

// @genReq:"取消匹配"
// @linkResp:"Match_CancelMatchResp"
type Match_CancelMatch struct {
}

// @genResp:""
// @comment:"取消匹配返回"
type Match_CancelMatchResp struct {
	SyncResp
}

// @genResp:""
// @comment:"角色加入通知"
type Match_RoleJoinNotify struct {
	RoomId   string
	RoleList []*data.LB_PlayerInfo
}

// @genResp:""
// @comment:"角色离开通知"
type Match_RoleLeaveNotify struct {
	RoomId     string
	RoleIdList []int64
}

// @genResp:""
// @comment:"匹配成功通知"
type Match_MatchSuccessNotify struct {
	RoomId      string
	ServerRpcId string
	MapId       int32
	RoleList    []*data.LB_PlayerInfo
	RandSeed    int32
}

// @genResp:""
// @comment:"匹配AI成功通知"
type Match_MatchAISucess struct {
}

// @genReq:"获取匹配战场信息"
// @linkResp:"Match_GetBattleInfoResp"
type Match_GetBattleInfo struct {
}

// @genResp:""
// @comment:"获取匹配战场信息返回"
type Match_GetBattleInfoResp struct {
	RoomId      string                `comment:"房间id"`
	ServerRpcId string                `comment:"房间rpc id"`
	MapId       int32                 `comment:"地图 id"`
	RoleList    []*data.LB_PlayerInfo `comment:"玩家列表"`
}

// @genReq:"获取匹配战场信息"
// @linkResp:"SyncRewardResp"
type Match_GetRankReward struct {
	RankRewardId int32 `comment:"RankReward表id"`
}

// @genReq:""
// @linkResp:"Match_GetRankResp"
// @comment:"连线战斗段位分排名"
type Match_GetRank struct {
}

// @genResp:""
type Match_GetRankResp struct {
	List   []*data.GDRankLBScoreMember `comment:"排名列表"`
	MyRank *data.GDRankLBScoreMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:""
type Match_ChangeSoldier struct {
	Id int32 `comment:"解锁过的士兵id"`
}
