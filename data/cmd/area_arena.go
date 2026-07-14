package cmd

import (
	"evo-data/data"

	data2 "git.hortorgames.com/orange/mandarin/v3/modules/leaderboard/data"
)

// @genReq:""
// @linkResp:"Arena_StartAreaResp"
// @comment:"跨服竞技场开始分区"
type Arena_StartArea struct{}

// @genResp:""
// @comment:"跨服竞技场开始分区返回"
type Arena_StartAreaResp struct {
	AreaArena  *data.GDAreaArena `comment:"跨服竞技场信息"`
	IsNewPhase bool              `comment:"是否为新一期"`
}

// @genReq:""
// @linkResp:"Arena_GetAreaTargetResp"
// @comment:"获取跨服竞技场目标"
type Arena_GetAreaTarget struct {
	Refresh bool `comment:"是否刷新"`
}

// @genResp:""
// @comment:"获取跨服竞技场目标返回"
type Arena_GetAreaTargetResp struct {
	RoleList []*data2.LeaderBoardRoleInfo
	Role     *data.GDRole `comment:"角色信息"`
}

// @genReq:""
// @linkResp:"Arena_GetAreaRankResp"
// @comment:"获取跨服竞技场排名"
type Arena_GetAreaRank struct {
	RankType data.EMAreaArenaRankType `comment:"排行榜类型"`
	Start    int32                    `comment:"拉榜起始名次(>=1)；0 表示从第 1 名起"`
	End      int32                    `comment:"拉榜终止名次(含)；0 表示默认一页；服务端最大 200"`
}

// @genResp:""
// @comment:"获取跨服竞技场排名返回"
type Arena_GetAreaRankResp struct {
	List          []*data.GDRankMember `comment:"排名列表"`
	MyRank        *data.GDRankMember   `comment:"自己的排名"`
	LastLevelRank *data.GDRankMember   `comment:"上一档排名"`
	Total         int64                `comment:"排行榜总人数"`
}

// @genReq:""
// @linkResp:"Arena_GetBattleRecordResp"
// @comment:"获取战斗记录"
type Arena_GetBattleRecord struct{}

// @genResp:""
type Arena_GetBattleRecordResp struct {
	List []*data.GDArenaRecord `comment:"记录列表"`
}

// @genReq:""
// @linkResp:"Arena_GetGoodsListResp"
// @comment:"获取商品列表"
type Arena_GetGoodsList struct{}

// @genResp:""
type Arena_GetGoodsListResp struct {
	GoodsList map[int32]int32 `comment:"记录列表"`
}

// @genReq:""
// @linkResp:"Arena_BuyGoodsResp"
// @comment:"购买商品"
type Arena_BuyGoods struct {
	GoodsId int32 `comment:"商品编号"`
	Count   int32 `comment:"购买数量"`
}

// @genResp:""
type Arena_BuyGoodsResp struct {
	SyncRewardResp
	GoodsList map[int32]int32 `comment:"记录列表"`
}

// @genReq:""
// @linkResp:"SyncResp"
// @comment:"购买商品"
type Arena_BuyTicket struct {
}
