package cmd

import "evo-data/data"

// @genResp:""
// @comment:"活动文档增量同步（与 SyncResp 并列，勿塞进 Role）"
type SyncRoleActivityResp struct {
	SyncResp
	Activity *data.GDActivity `comment:"角色活动数据"`
}

// @genReq:""
// @linkResp:"Activity_GetResp"
// @comment:"拉取全量玩家活动文档（actNewbie 可能为空：未达开启条件时不会创建子文档）"
type Activity_Get struct {
}

// @genResp:""
// @comment:"全量玩家活动文档（含根与已有子活动）"
type Activity_GetResp struct {
	SyncRoleActivityResp
}

// @genResp:""
// @comment:"活动文档增量同步"
type Activity_Notify struct {
	SyncRoleActivityResp
}

// @genReq:""
// @linkResp:"Activity_NewbiePowerClaimResp"
// @comment:"「群雄并起」战力活动领取档位奖励"
type Activity_NewbiePowerClaim struct {
	ConfId int32 `comment:"NewbiePowerActConf.Id"`
}

// @genResp:""
// @comment:"战力领取返回：角色增量 + 奖励 + 活动根增量（勿双匿名嵌入以免代码生成丢字段）"
type Activity_NewbiePowerClaimResp struct {
	SyncRoleActivityResp
	Reward []*data.GDRewardData `comment:"本次奖励"`
}

// @genReq:""
// @linkResp:"Activity_NewbieRecruitClaimResp"
// @comment:"「群雄并起」转盘档位奖励领取"
type Activity_NewbieRecruitClaim struct {
	ConfId int32 `comment:"NewbieRecruitActConf.Id"`
}

// @genResp:""
type Activity_NewbieRecruitClaimResp struct {
	SyncRoleActivityResp
	Reward []*data.GDRewardData `comment:"本次奖励"`
}

// @genReq:""
// @linkResp:"Activity_NewbieRecruitGachaResp"
// @comment:"「群雄并起」转盘抽奖"
type Activity_NewbieRecruitGacha struct {
	Count int32 `comment:"抽奖次数"`
}

// @genResp:""
type Activity_NewbieRecruitGachaResp struct {
	SyncRoleActivityResp
	Indexes []*int32             `comment:"抽奖奖励索引"`
	Reward  []*data.GDRewardData `comment:"本次奖励"`
}

// @genReq:""
// @linkResp:"Activity_NewbieRetriggerResp"
// @comment:"[仅 debug 环境] 重置 GDActNewbie（清空档位/次数/回收标记并设置 StartTime），供测试快速对齐活动时间窗"
type Activity_NewbieRetrigger struct {
	StartTime int32 `comment:"活动开始时间戳（Unix 秒）"`
}

// @genResp:""
type Activity_NewbieRetriggerResp struct {
	SyncRoleActivityResp
}

// @genReq:""
// @linkResp:"Activity_NewbieClaimGiftResp"
// @comment:"领取「群雄并起」礼包（含每日免费道具）"
type Activity_NewbieClaimGift struct {
	GiftId int32 `comment:"礼包id"`
	Count  int32 `comment:"购买数量"`
}

// @genResp:""
type Activity_NewbieClaimGiftResp struct {
	SyncRoleActivityResp
	Reward []*data.GDRewardData `comment:"本次奖励"`
}

// @genReq:""
// @linkResp:"Activity_BestPlayerClaimResp"
// @comment:"「最强领主」领取档位奖励"
type Activity_BestPlayerClaim struct {
	ConfId int32 `comment:"BestPlayerTargetConf.Id"`
}

// @genResp:""
// @comment:"领取返回：角色增量 + 奖励 + 活动根增量（勿双匿名嵌入以免代码生成丢字段）"
type Activity_BestPlayerClaimResp struct {
	SyncRoleActivityResp
	Reward []*data.GDRewardData `comment:"本次奖励"`
}

// @genReq:""
// @linkResp:"Activity_BestPlayerDailyRankResp"
// @comment:"获取最强领主日排名"
type Activity_BestPlayerDailyRank struct {
	RealServerId int64  `comment:"0=当前角色真实服；非0=按该服共榜组查榜（调试用）"`
	Phase        string `comment:"空=活动文档期号；非空=指定 Redis 期号（调试用）"`
	ActivityDay  int32  `comment:"0=当前活动日；1..N=活动内第N天日榜（调试用）"`
	Start        int32  `comment:"拉榜起始名次(>=1)；0 表示从第 1 名起"`
	End          int32  `comment:"拉榜终止名次(含)；0 表示默认一页；服务端最大 200"`
}

// @genResp:""
type Activity_BestPlayerDailyRankResp struct {
	RankName string               `comment:"排行榜名称(debug用)"`
	List     []*data.GDRankMember `comment:"排名列表"`
	MyRank   *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Activity_BestPlayerFinalRankResp"
// @comment:"获取最强领主总排名"
type Activity_BestPlayerFinalRank struct {
	RealServerId int64  `comment:"0=当前角色真实服；非0=按该服共榜组查榜（调试用）"`
	Phase        string `comment:"空=活动文档期号；非空=指定 Redis 期号（调试用）"`
	Start        int32  `comment:"拉榜起始名次(>=1)；0 表示从第 1 名起"`
	End          int32  `comment:"拉榜终止名次(含)；0 表示默认一页；服务端最大 200"`
}

// @genResp:""
type Activity_BestPlayerFinalRankResp struct {
	RankName string               `comment:"排行榜名称(debug用)"`
	List     []*data.GDRankMember `comment:"排名列表"`
	MyRank   *data.GDRankMember   `comment:"自己的排名"`
}

// @genReq:""
// @linkResp:"Activity_BestPlayerClaimGiftResp"
// @comment:"领取「最强领主」日礼包免费档（无道具兑换，每次固定 1 份、自然日一次）；人民币档位走充值 ChargeType=ActBestPlayer"
type Activity_BestPlayerClaimGift struct {
	GiftId int32 `comment:"礼包池子 Id（GiftRootPool.Id）；当日白名单以 BestPlayerDayConf.Gift 与池子 ThemeSort 对应为准"`
}

// @genResp:""
type Activity_BestPlayerClaimGiftResp struct {
	SyncRoleActivityResp
	Reward []*data.GDRewardData `comment:"本次奖励"`
}

// @genReq:""
// @linkResp:"Activity_BestPlayerReissueDailyRankResp"
// @comment:"领取「最强领主」日礼包免费档（无道具兑换，每次固定 1 份、自然日一次）；人民币档位走充值 ChargeType=ActBestPlayer"
type Activity_BestPlayerReissueDailyRank struct {
	AreaId int64  `comment:"合服 ServerId，主服"`
	Phase  string `comment:"对应军团战期号（与 GDLWarStateInfo.Phase 一致）"`
	Day    int32  `comment:"检查补发第几日的排行榜奖励"`
}

// @genResp:""
type Activity_BestPlayerReissueDailyRankResp struct {
	Msg string `comment:"message"`
}

// @genReq:""
// @linkResp:"Activity_BestPlayerReissueFinalRankResp"
// @comment:"领取「最强领主」日礼包免费档（无道具兑换，每次固定 1 份、自然日一次）；人民币档位走充值 ChargeType=ActBestPlayer"
type Activity_BestPlayerReissueFinalRank struct {
	AreaId int64  `comment:"合服 ServerId，主服"`
	Phase  string `comment:"对应军团战期号（与 GDLWarStateInfo.Phase 一致）"`
}

// @genResp:""
type Activity_BestPlayerReissueFinalRankResp struct {
	Msg string `comment:"message"`
}

// @genReq:""
// @linkResp:"Activity_ServerBattleInfoResp"
// @comment:"服务器对决详细数据请求（面板信息）"
type Activity_ServerBattleInfo struct {
}

// @genResp:""
type Activity_ServerBattleInfoResp struct {
	Open          bool  `comment:"活动是否开启中"`
	RivalServerId int32 `comment:"对手 ServerId"`
	StartTime     int64 `comment:"开始时间"`
	EndTime       int64 `comment:"开始时间"`
	MyScore       int64 `comment:"我的服务器积分"`
	RivalScore    int64 `comment:"对手的服务器积分"`
}
