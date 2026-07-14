package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Rank_GetServerRankResp"
// @comment:"获取服务器排名"
type Rank_GetServerRank struct {
}

// @genResp:""
type Rank_GetServerRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Rank_GetTowerRankResp"
// @comment:"获取爬塔排名"
type Rank_GetTowerRank struct {
}

// @genResp:""
type Rank_GetTowerRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Rank_GetLBScoreRankResp"
// @comment:"获取段位分排名"
type Rank_GetLBScoreRank struct {
}

// @genResp:""
type Rank_GetLBScoreRankResp struct {
	List   []*data.GDRankLBScoreMember `comment:"排名列表"`
	MyRank *data.GDRankLBScoreMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Rank_GetRoleInfoResp"
// @comment:"获取角色信息"
type Rank_GetRoleInfo struct {
	RoleId int64 `comment:"角色编号"`
}

// @genResp:""
type Rank_GetRoleInfoResp struct {
	RoleInfo *data.GDRole `comment:"角色信息"`
}

// @genReq:""
// @linkResp:"Rank_GetBossRankResp"
// @comment:"获取蛙王排名"
type Rank_GetBossRank struct {
}

// @genResp:""
type Rank_GetBossRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Rank_GetConsumeRankResp"
// @comment:"获取消耗排名"
type Rank_GetConsumeRank struct {
}

// @genResp:""
type Rank_GetConsumeRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Rank_GetPowerRankResp"
// @comment:"获取战力排名"
type Rank_GetPowerRank struct {
}

// @genResp:""
type Rank_GetPowerRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}
