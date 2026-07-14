package data

import (
	"time"
)

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"57600"`
// @redis:`routeType:"legion" routeBy:"id" idField:"id" key:"legion" omitempty:"true"`
// @mongodb:`routeType:"legion" routeBy:"id" idField:"id" collection:"legions" enableSharding:"true"`
// @comment:`value:"军团"`
type GDLegion struct {
	Id             int64                                      `comment:"军团id" mongodb:"index,unique"`
	Name           string                                     `comment:"军团名称" mongodb:"index"`
	ServerId       int64                                      `comment:"服务器编号"`
	Announcement   string                                     `comment:"军团公告"`
	Exp            int32                                      `comment:"军团经验"`
	Level          int32                                      `comment:"军团等级"`
	Dan            int32                                      `comment:"军团段位"`
	Logo           string                                     `comment:"军团标志"`
	LeaderId       int64                                      `comment:"军团首领"`
	Members        map[int64]*GDLegionMember                  `comment:"军团成员"`
	Logs           []*GDLegionLog                             `comment:"军团日志"`
	Founder        int64                                      `comment:"创始人编号"`
	Statistics     map[string]int64                           `comment:"数据、统计"`
	StatisticsTime map[string]int64                           `comment:"数据、统计的时间"`
	IsDeleted      bool                                       `comment:"是否解散"`
	Custom         map[string]string                          `comment:"自定义数据"`
	CreatedTime    time.Time                                  `comment:"创建时间"`
	DeletedTime    time.Time                                  `comment:"解散时间"`
	ItemMap        map[int32]*GDLegionItem                    `comment:"道具"`
	NameplateType  EMNameplateType                            `comment:"使用的铭牌类型"`
	Nameplate      map[EMNameplateType]map[int32]*GDNameplate `comment:"拥有的铭牌信息"`
	SkinMap        map[int32]int32                            `comment:"穿戴皮肤"`
	NoApply        bool                                       `comment:"免申请"`
}

// @genEnum:`value:"true"`
// @comment:`value:"军团职位"`
type EMLegionJob struct {
	Leader    int32 `enum:"1" comment:"首领"`
	SbuLeader int32 `enum:"2" comment:"副首领"`
	Member    int32 `enum:"999" comment:"成员"`
}

// @genData:`value:"true"`
// @comment:`value:"军团成员"`
type GDLegionMember struct {
	RoleId         int64                  `comment:"角色编号"`
	Name           string                 `comment:"角色名称"`
	HeadImg        string                 `comment:"角色头像"`
	Job            EMLegionJob            `comment:"军团职位"`
	Power          int64                  `comment:"战力"`
	LordLvl        int32                  `comment:"主公等级"`
	OriginServerId int64                  `comment:"原始服务器编号"`
	Medallion      []int32                `comment:"徽章"`
	Custom         map[string]interface{} `comment:"自定义数据"`
	SignInTime     int64                  `comment:"签到时间"`
	Online         int64                  `comment:"是否在线"`
	JoinTime       time.Time              `comment:"加入时间"`
	LordSkin       int32                  `comment:"主公皮肤"`
	AvatarFrame    int32                  `comment:"装扮头像框"`
	PlayerCard     int32                  `comment:"vip卡片"`
}

// @genData:`value:"true"`
// @comment:`value:"军团日志"`
type GDLegionLog struct {
	Content    string `comment:"日志内容"`
	RoleId     int64  `comment:"角色编号"`
	CreateTime int64  `comment:"创建时间"`
}

// @genData:`value:"true"`
// @comment:`value:"军团排名"`
type GDRankLegion struct {
	Id            int64                                      `comment:"军团ID"`
	Name          string                                     `comment:"军团名称"`
	ServerId      int64                                      `comment:"服务器编号"`
	Level         int32                                      `comment:"军团等级"`
	Logo          string                                     `comment:"军团标志"`
	MemberCount   int64                                      `comment:"军团成员数量"`
	Rank          int32                                      `comment:"排名"`
	Score         float64                                    `comment:"排名分数"`
	NameplateType EMNameplateType                            `comment:"使用的铭牌类型"`
	Nameplate     map[EMNameplateType]map[int32]*GDNameplate `comment:"拥有的铭牌信息"`
	UpdateTime    int64                                      `comment:"更新时间" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"军团道具"`
type GDLegionItem struct {
	Id          int32 `comment:"军团道具 id"`
	Quantity    int32 `comment:"军团道具数量"`
	CreatedTime int64 `comment:"创建时间"`
	DeletedTime int64 `comment:"过期时间 -1 为永久"`
}

// @genData:`value:"true"`
// @comment:`value:"军团信息"`
type GDLegionData struct {
	Id            int64                                      `comment:"军团编号"`
	Name          string                                     `comment:"军团名称"`
	Announcement  string                                     `comment:"军团公告"`
	Level         int32                                      `comment:"军团等级"`
	Logo          string                                     `comment:"军团标志"`
	LeaderId      int64                                      `comment:"军团首领"`
	ServerId      int64                                      `comment:"服务器编号"`
	NoApply       bool                                       `comment:"免申请"`
	TotalPower    int64                                      `comment:"总战力"`
	Members       map[int64]*GDLegionMember                  `comment:"军团成员"`
	NameplateType EMNameplateType                            `comment:"使用的铭牌类型"`
	Nameplate     map[EMNameplateType]map[int32]*GDNameplate `comment:"拥有的铭牌信息"`
}

// @genData:`value:"true"`
// @comment:`value:"铭牌"`
type GDNameplate struct {
	Id         int32 `comment:"铭牌编号"`
	ExpireTime int64 `comment:"铭牌过期时间"`
	BeginTime  int64 `comment:"铭牌开始显示时间"`
}

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"铭牌类型"`
type EMNameplateType struct {
	None       int32 `enum:"0" comment:"无"`
	LegionWar  int32 `enum:"1" comment:"争霸赛"`
	LegionRank int32 `enum:"2" comment:"排位赛"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"main" routeBy:"name" idField:"name" collection:"legion_name" enableSharding:"true" useMongoObjectID:"true"`
// @comment:`value:"军团名称"`
type GDLegionName struct {
	Name string `comment:"军团名称" mongodb:"index,unique"`
}

// @genData:`value:"true"`
// @comment:`value:"军团新标签"`
type GDNewLegionTags struct {
	Tags map[int32]*GDNewLegionTagInfo `comment:"标签，key是标签类型"`
}

// @genData:`value:"true"`
// @comment:`value:"军团新标签信息"`
type GDNewLegionTagInfo struct {
	TagId  int32   `comment:"标签ID"`
	Params []int32 `comment:"标签参数"`
}

// @genEnum:`value:"true"`
// @comment:`value:"修改军团信息类型"`
type EMLegionInfoType struct {
	NoApply int32 `enum:"0" comment:"免申请"`
}

// @genData:`value:"true"`
// @comment:`value:"宝箱简要信息"`
type GDLegionBoxItem struct {
	Id                string `comment:"宝箱唯一ID"`
	ConfigId          int32  `comment:"宝箱配置ID"`
	ClaimCnt          int32  `comment:"已领取人数"`
	SenderRoleId      int64  `comment:"发送者uid"`
	SenderName        string `comment:"发送者昵称"`
	SenderHeadImg     string `comment:"发送者头像"`
	SenderAvatarFrame int32  `comment:"发送者头像框"`
	CreateAt          int64  `comment:"创建时间"`
	ExpireAt          int64  `comment:"过期时间"`
	Claimed           bool   `comment:"当前玩家是否已领取"`
}

// @genData:`value:"true"`
// @comment:`value:"领取记录简要"`
type GDLegionBoxClaimRecord struct {
	RoleId      int64           `comment:"角色ID"`
	Nickname    string          `comment:"领取人昵称"`
	HeadImg     string          `comment:"领取人头像"`
	AvatarFrame int32           `comment:"领取人头像框"`
	ClaimedAt   int64           `comment:"领取时间"`
	RewardList  []*GDRewardData `comment:"奖励列表"`
}
