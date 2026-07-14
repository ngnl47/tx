package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Legion_RecommendResp"
// @comment:"推荐军团"
type Legion_Recommend struct {
	IsHistory bool `comment:"是否获取历史军团"`
}

// @genResp:""
// @comment:"推荐军团返回"
type Legion_RecommendResp struct {
	List      []*data.GDLegion                `comment:"军团列表"`
	ApplyList map[int64]int                   `comment:"申请过的军团列表"`
	TagsMap   map[int64]*data.GDNewLegionTags `comment:"标签"`
}

// @genReq:""
// @linkResp:"Legion_SearchResp"
// @comment:"搜索军团"
type Legion_Search struct {
	Key string `comment:"搜索关键字"`
}

// @genResp:""
// @comment:"搜索军团返回"
type Legion_SearchResp struct {
	List      []*data.GDLegion                `comment:"军团列表"`
	ApplyList map[int64]int                   `comment:"申请过的军团列表"`
	TagsMap   map[int64]*data.GDNewLegionTags `comment:"标签"`
}

// @genReq:""
// @linkResp:"Legion_GetInfoResp"
// @comment:"获取军团信息"
type Legion_GetInfo struct{}

// @genResp:""
// @comment:"获取军团信息返回"
type Legion_GetInfoResp struct {
	Info *data.GDLegion `comment:"军团信息"`
}

// @genReq:""
// @linkResp:"Legion_CreateResp"
// @comment:"创建军团"
type Legion_Create struct {
	Name         string `comment:"军团名称"`
	Announcement string `comment:"军团公告"`
	NoApply      bool   `comment:"免申请"`
}

// @genResp:""
// @comment:"创建军团返回"
type Legion_CreateResp struct {
	Legion_GetInfoResp
	Role *data.GDRole `comment:"角色信息"`
}

// @genReq:""
// @linkResp:"Legion_ApplyJoinResp"
// @comment:"申请加入军团"
type Legion_ApplyJoin struct {
	LegionId int64  `comment:"军团编号"`
	Reason   string `comment:"申请理由"`
}

// @genResp:""
// @comment:"申请加入军团返回"
type Legion_ApplyJoinResp struct {
	SyncResp
	ApplyList map[int64]int  `comment:"申请过的军团列表"`
	Legion    *data.GDLegion `comment:"军团信息"`
}

// @genReq:""
// @linkResp:"Legion_ApplyListResp"
// @comment:"申请加入军团列表"
type Legion_ApplyList struct {
}

// @genResp:""
// @comment:"申请加入军团列表返回"
type Legion_ApplyListResp struct {
	RoleList []*data.GDSimpleRole `comment:"申请用户列表"`
}

// @genResp:""
// @comment:"刷新申请加入军团列表"
type RefreshApplyListNotify struct {
	Legion_ApplyListResp
}

// @genReq:""
// @linkResp:"Legion_AgreeResp"
// @comment:"同意加入军团"
type Legion_Agree struct {
	RoleId int64 `comment:"用户编号"`
}

// @genResp:""
// @comment:"同意加入军团返回"
type Legion_AgreeResp struct {
	Legion_ApplyListResp
	Info *data.GDLegion `comment:"军团信息"`
}

// @genReq:""
// @linkResp:"Legion_IgnoreResp"
// @comment:"忽略加入军团"
type Legion_Ignore struct {
	RoleId int64 `comment:"用户编号"`
}

// @genResp:""
// @comment:"忽略加入军团返回"
type Legion_IgnoreResp struct {
	Legion_ApplyListResp
}

// @genReq:""
// @linkResp:"Legion_EditNameResp"
// @comment:"军团名称修改"
type Legion_EditName struct {
	Name string `comment:"军团名称"`
}

// @genResp:""
// @comment:"军团名称修改返回"
type Legion_EditNameResp struct {
	Legion_GetInfoResp
	Role *data.GDRole `comment:"角色信息"`
}

// @genReq:""
// @linkResp:"Legion_ChangeJobResp"
// @comment:"职位变更"
type Legion_ChangeJob struct {
	RoleId int64            `comment:"用户编号"`
	Job    data.EMLegionJob `comment:"军团职位"`
}

// @genResp:""
// @comment:"职位变更返回"
type Legion_ChangeJobResp struct {
	Legion_GetInfoResp
}

// @genReq:""
// @linkResp:"Legion_KickOutResp"
// @comment:"踢出军团"
type Legion_KickOut struct {
	RoleId int64 `comment:"用户编号"`
}

// @genResp:""
// @comment:"踢出军团返回"
type Legion_KickOutResp struct {
	Legion_GetInfoResp
}

// @genReq:""
// @linkResp:"Legion_EditNameResp"
// @comment:"军团公告修改"
type Legion_EditAnnouncement struct {
	Announcement string `comment:"军团公告"`
}

// @genResp:""
// @comment:"军团公告修改返回"
type Legion_EditAnnouncementResp struct {
	Role *data.GDRole `comment:"角色信息"`
	Legion_GetInfoResp
}

// @genReq:""
// @linkResp:"Legion_LeaveResp"
// @comment:"退出军团"
type Legion_Leave struct{}

// @genResp:""
// @comment:"退出军团返回"
type Legion_LeaveResp struct {
	SyncResp
}

// @genReq:""
// @linkResp:"Legion_LeaveResp"
// @comment:"解散军团"
type Legion_Dissolve struct{}

// @genResp:""
// @comment:"解散军团返回"
type Legion_DissolveResp struct {
	SyncResp
}

// @genReq:""
// @linkResp:"Legion_SignInResp"
// @comment:"军团签到"
type Legion_SignIn struct{}

// @genResp:""
// @comment:"军团签到返回"
type Legion_SignInResp struct {
	Legion_GetInfoResp
	Role   *data.GDRole         `comment:"角色信息"`
	Reward []*data.GDRewardData `comment:"签到奖励"`
}

// @genReq:""
// @linkResp:"Legion_SendMailResp"
// @comment:"发送军团邮件"
type Legion_SendMail struct {
	Content string `comment:"邮件内容"`
}

// @genResp:""
// @comment:"发送军团邮件返回"
type Legion_SendMailResp struct {
	Legion_GetInfoResp
}

// @genResp:""
type LegionNotify struct {
	Legion_GetInfoResp
}

// @genReq:""
// @linkResp:"Legion_GetLogsResp"
// @comment:"获取军团log"
type Legion_GetLogs struct{}

// @genResp:""
// @comment:"获取军团log返回"
type Legion_GetLogsResp struct {
	Logs []*data.GDLegionLog `comment:"日志列表"`
}

// @genReq:""
// @linkResp:"Legion_StoreGoodsListResp"
// @comment:"军团商店商品列表"
type Legion_StoreGoodsList struct {
}

// @genResp:""
// @comment:"军团商店商品列表返回"
type Legion_StoreGoodsListResp struct {
	BuyGoods map[int32]int32 `comment:"购买商品信息(Key是编号;Value是购买数量)"`
}

// @genReq:""
// @linkResp:"Legion_StoreBuyGoodsResp"
// @comment:"购买军团商店商品"
type Legion_StoreBuyGoods struct {
	Id int32 `comment:"编号"`
}

// @genResp:""
// @comment:"购买军团商店商品返回"
type Legion_StoreBuyGoodsResp struct {
	SyncRewardResp
	BuyGoods map[int32]int32 `comment:"购买商品信息(Key是编号;Value是购买数量)"`
}

// @genReq:""
// @linkResp:"Legion_ScoreShopBuyResp"
// @comment:"积分商城购买物品"
type Legion_ScoreShopBuy struct {
	ShopId  int32 `comment:"积分商城 id"`
	BuyType int32 `comment:"购买类型 1 限时 2 永久"`
}

// @genResp:""
// @comment:"积分商城购买物品反馈"
type Legion_ScoreShopBuyResp struct {
	Legion_GetInfoResp
	ShopId     int32 `comment:"积分商城 id"`
	BuyType    int32 `comment:"购买类型 1 限时 2 永久"`
	RewardList []*data.GDLegionRewardData
}

// @genReq:""
// @linkResp:"Legion_GetInfoByIdResp"
// @comment:"根据编号获取军团信息"
type Legion_GetInfoById struct {
	LegionId int64 `comment:"军团编号"`
}

// @genResp:""
// @comment:"根据编号获取军团信息返回"
type Legion_GetInfoByIdResp struct {
	LegionData *data.GDLegionData `comment:"军团信息"`
	Apply      bool               `comment:"是否申请中"`
}

// @genReq:""
// @linkResp:"Legion_SyncLogoResp"
// @comment:"手动刷新军团logo"
type Legion_SyncLogo struct{}

// @genResp:""
// @comment:"手动刷新军团logo返回"
type Legion_SyncLogoResp struct {
	Info *data.GDLegion `comment:"军团信息"`
}

// @genResp:""
// @comment:"刷新军团聊天信息"
type RefreshLegionChatMessageNotify struct {
	LegionId int64 `comment:"军团id" `
}

// @genResp:""
// @comment:"军团信息通知"
type Legion_InfoNotify struct {
	LegionId int64          `comment:"军团id"`
	Info     *data.GDLegion `comment:"军团信息"`
}

// @genReq:""
// @linkResp:"Legion_EditInfoResp"
// @comment:"军团修改信息"
type Legion_EditInfo struct {
	Typ     data.EMLegionInfoType `comment:"修改类型"`
	NoApply bool                  `comment:"免申请"`
}

// @genResp:""
// @comment:"军团修改信息返回"
type Legion_EditInfoResp struct {
	Legion *data.GDLegion `comment:"军团信息"`
}

// @genReq:""
// @linkResp:"Legion_DirectJoinResp"
// @comment:"一键加入军团"
type Legion_DirectJoin struct {
}

// @genResp:""
// @comment:"一键加入军团返回"
type Legion_DirectJoinResp struct {
	SyncResp
	Legion *data.GDLegion `comment:"军团信息"`
}

// @genReq:""
// @linkResp:"Legion_GetServerRankResp"
// @comment:"获取本服军团排名"
type Legion_GetServerRank struct{}

// @genResp:""
// @comment:"获取本服军团排名返回"
type Legion_GetServerRankResp struct {
	List   []*data.GDRankLegion `comment:"排名列表"`
	MyRank *data.GDRankLegion   `comment:"自己的排名"`
}
