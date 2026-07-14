package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"LegionWar_GetBfInfoResp"
// @comment:"获取战场信息"
type LegionWar_GetBfInfo struct {
	BuildId int64 `comment:"建筑id"`
}

// @genResp:""
// @comment:"获取战场信息返回"
type LegionWar_GetBfInfoResp struct {
	BF            *data.GDBattlefield
	ViewBuildId   int64                 `comment:"视野建筑id"`
	LegionWarInfo *data.GDLegionWarInfo `comment:"军团战争信息"`
	NextOpenTime  int64                 `comment:"下次开启时间"`
	MergedBF      bool                  `comment:"是否是合服战场"`
}

// @genReq:""
// @linkResp:"LegionWar_SignUpResp"
// @comment:"报名"
type LegionWar_SignUp struct {
}

// @genResp:""
// @comment:"报名返回"
type LegionWar_SignUpResp struct {
	BF            *data.GDBattlefield
	LegionWarInfo *data.GDLegionWarInfo `comment:"军团战争信息"`
}

// @genReq:""
// @linkResp:"LegionWar_SubsResp"
// @comment:"约战"
type LegionWar_Subs struct {
	BuildId int64 `comment:"建筑id"`
}

// @genResp:""
// @comment:"约战返回"
type LegionWar_SubsResp struct {
	BF            *data.GDBattlefield
	LegionWarInfo *data.GDLegionWarInfo `comment:"军团战争信息"`
}

// @genReq:""
// @linkResp:"LegionWar_CancelSubsResp"
// @comment:"取消约战"
type LegionWar_CancelSubs struct {
	BuildId int64 `comment:"建筑id"`
}

// @genResp:""
// @comment:"取消约战返回"
type LegionWar_CancelSubsResp struct {
	BF            *data.GDBattlefield
	LegionWarInfo *data.GDLegionWarInfo `comment:"军团战争信息"`
}

// @genReq:""
// @linkResp:"LegionWar_AbandonBuildResp"
// @comment:"放弃建筑"
type LegionWar_AbandonBuild struct {
	BuildId int64 `comment:"建筑id"`
}

// @genResp:""
// @comment:"放弃建筑返回"
type LegionWar_AbandonBuildResp struct {
	BF            *data.GDBattlefield
	LegionWarInfo *data.GDLegionWarInfo `comment:"军团战争信息"`
}

// @genReq:""
// @linkResp:"LegionWar_GetDetailsResp"
// @comment:"获取详情"
type LegionWar_GetDetails struct {
	Date string `comment:"日期"`
}

// @genResp:""
// @comment:"获取详情返回"
type LegionWar_GetDetailsResp struct {
	RoleDetailsList []*data.GDLWBattleDetails `comment:"玩家战斗数据"`
}

// @genReq:""
// @linkResp:"LegionWar_GetBattleRecordResp"
// @comment:"获取战报"
type LegionWar_GetBattleRecord struct {
	RecordId string `comment:"记录ID"`
}

// @genResp:""
// @comment:"获取战报返回"
type LegionWar_GetBattleRecordResp struct {
	Record *data.GDBattle `comment:"战斗记录"`
}

// @genReq:""
// @linkResp:"LegionWar_GetLegionScoreRankResp"
// @comment:"获取本服军团积分排名"
type LegionWar_GetLegionScoreRank struct{}

// @genResp:""
// @comment:"获取本服军团积分排名返回"
type LegionWar_GetLegionScoreRankResp struct {
	List   []*data.GDRankLegion `comment:"排名列表"`
	MyRank *data.GDRankLegion   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"LegionWar_GetServerControlAreaRankResp"
// @comment:"获取服务器级控制区汇总排行榜（各服控制区积分汇总）"
type LegionWar_GetServerControlAreaRank struct {
	RealServerId int64 `comment:"目标服 RealServerId（战场 bfId 与服级榜按此服计算，不使用角色所属服）"`
}

// @genResp:""
// @comment:"获取服务器级控制区汇总排行榜返回"
type LegionWar_GetServerControlAreaRankResp struct {
	List   []*data.GDRankServer `comment:"排名列表（条目 Id/ServerId 为 RealServerId）"`
	MyRank *data.GDRankServer   `comment:"请求 RealServerId 对应条目"`
}

// @genReq:""
// @linkResp:"LegionWar_InitServerControlAreaRankResp"
// @comment:"手动首次初始化/全量校正服务器级控制区汇总榜（从军团控制区榜汇总写入）"
type LegionWar_InitServerControlAreaRank struct {
	RealServerId int64 `comment:"目标服 RealServerId；-1 表示遍历当前所有服，相同军团战 areaId 仅校正一次"`
}

// @genResp:""
// @comment:"手动初始化返回"
type LegionWar_InitServerControlAreaRankResp struct {
	Msg     string  `comment:"执行摘要说明"`
	AreaIds []int64 `comment:"实际执行了校正的军团战 areaId 列表"`
}

// @genReq:""
// @linkResp:"LegionWar_GetPersonalScoreRankResp"
// @comment:"获取本服个人积分排名"
type LegionWar_GetPersonalScoreRank struct{}

// @genResp:""
// @comment:"获取本服个人积分排名返回"
type LegionWar_GetPersonalScoreRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"LegionWar_GetPSInLegionRankResp"
// @comment:"获取团内积分排名"
type LegionWar_GetPSInLegionRank struct{}

// @genResp:""
// @comment:"获取团内积分排名返回"
type LegionWar_GetPSInLegionRankResp struct {
	List   []*data.GDRankMember `comment:"排名列表"`
	MyRank *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取占领城市的每日奖励"
type LegionWar_GetCityRwd struct {
	CityId int64 `comment:"城市配置Id"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"获取占领城市的每日奖励红点"
type LegionWar_GetCityRwdRedDot struct {
}

// @genReq:""
// @linkResp:"SyncRewardResp"
// @comment:"获取休战期每日奖励"
type LegionWar_GetGapRwd struct {
}
