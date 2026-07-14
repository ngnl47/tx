package data

// @genEnum:`value:"true"`
// @comment:`value:"聊天消息频道类型"`
type EMChatMsgChannelType struct {
	World     int32 `enum:"1" comment:"世界"`
	Legion    int32 `enum:"2" comment:"俱乐部"`
	Server    int32 `enum:"3" comment:"本服"`
	Friend    int32 `enum:"4" comment:"好友"`
	LegionWar int32 `enum:"5" comment:"军团战"`
}

// @genEnum:`value:"true"`
// @comment:`value:"聊天消息类型"`
type EMChatMsgType struct {
	Simple  int32 `enum:"1" comment:"普通消息"`
	Emoji   int32 `enum:"2" comment:"表情消息"`
	Time    int32 `enum:"50" comment:"时间消息"`
	Retract int32 `enum:"100" comment:"撤回消息"`
	Update  int32 `enum:"200" comment:"更新消息"`
	Car     int32 `enum:"300" comment:"车辆消息"`
}

// @genEnum:`value:"true"`
// @comment:`value:"消息监控频道类型"`
type EMContentMonitorChannelType struct {
	None      int32 `enum:"0" comment:"未知"`
	World     int32 `enum:"1" comment:"世界"`
	AreaArena int32 `enum:"3" comment:"跨服竞技场"`
	Legion    int32 `enum:"4" comment:"俱乐部"`
	Private   int32 `enum:"5" comment:"私聊"`
	Recruit   int32 `enum:"6" comment:"其他"`
	Server    int32 `enum:"7" comment:"本服"`
}

// @genData:`value:"true"`
// @redis:`routeType:"chat" routeBy:"id" idField:"id" key:"chat"`
// @storage:`mode:"redis"`
// @comment:`value:"聊天消息"`
type GDChatMessage struct {
	Id           int64
	RoleId       int64                `comment:"角色ID"`
	RoleName     string               `comment:"角色昵称"`
	HeadImg      string               `comment:"角色头像"`
	Gender       int32                `comment:"玩家性别"`
	ServerId     int64                `comment:"服务器ID"`
	LegionId     int64                `comment:"军团ID"`
	LegionName   string               `comment:"军团名"`
	LegionMember int64                `comment:"军团城员数量"`
	Channel      EMChatMsgChannelType `comment:"消息频道"`
	MsgType      EMChatMsgType        `comment:"消息类型"`
	Msg          string               `comment:"消息文本"`
	Extra        *GDChatMsgExt        `comment:"额外信息"`
	EmojiId      int32                `comment:"表情编号"`
	CreateTime   int64                `comment:"创建时间"`
	AvatarFrame  int32                `comment:"头像框"`
}

// @genData:`value:"true"`
// @comment:`value:"聊天消息额外信息"`
type GDChatMsgExt struct {
	ExtType EMChatMsgExtType `comment:"额外信息分类"`
	Msg     any              `comment:"消息文本"`
}

// @genEnum:`value:"true"`
// @comment:`value:"聊天消息额外信息分类"`
type EMChatMsgExtType struct {
	MatchTeam int32 `enum:"1" comment:"组队信息"`
	BossTower int32 `enum:"2" comment:"咸王塔"`
	CardTower int32 `enum:"3" comment:"卡牌塔"`
	Car       int32 `enum:"4" comment:"货车"`
}

// @genData:`value:"true"`
// @comment:`value:"聊天消息额外信息"`
type GDChatMsgExtMatchTeam struct {
	TeamCfgId int32  `comment:"组队配置Id(对应MatchTeam表配置Id)"`
	TeamId    string `comment:"房间ID"`
	MemberCnt int    `comment:"成员数量"`
}
