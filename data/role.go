package data

import (
	"time"
)

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @redis:`routeType:"role" routeBy:"roleId" idField:"roleId" key:"role" omitempty:"true"`
// @mongodb:`routeType:"role" routeBy:"roleId" idField:"roleId" collection:"roles" enableSharding:"true"`
// @customExport:`value:"true"`
// @comment:`value:"角色"`
type GDRole struct {
	RoleId           int64                        `comment:"角色编号" mongodb:"index,unique"`
	Uid              int64                        `comment:"用户id" mongodb:"index"`
	OpenId           string                       `comment:"微信小游戏 openId" mongodb:"index"`
	Platform         string                       `comment:"平台" mongodb:"index"`
	PlatformExt      string                       `comment:"平台扩展"`
	PlatformId       int64                        `comment:"平台id" mongodb:"index"`
	PlatformUId      string                       `comment:"平台用户id" mongodb:"index"`
	Authed           bool                         `comment:"是否授权"`
	UKey             string                       `comment:"用户键值" mongodb:"index"`
	LoginPlatform    string                       `comment:"登录的平台"`
	LoginType        string                       `comment:"登录方式"`
	UserLoginType    string                       `comment:"登录方式"`
	GameTp           string                       `comment:"登陆平台，minigame为小游戏类型 app为APP类型"`
	AreaId           int64                        `comment:"大区编号" mongodb:"index::areaId:1:deleted:1:roleId:1"`
	RealServerId     int64                        `comment:"真实服务器编号"`
	ServerId         int64                        `comment:"显示的服务器编号" mongodb:"index"`
	ServerName       string                       `comment:"服务器名称"`
	LegionId         int64                        `comment:"军团id"`
	LegionName       string                       `comment:"军团名称"`
	LegionCdTime     int64                        `comment:"可以加入军团时间"`
	Name             string                       `comment:"玩家昵称" mongodb:"index"`
	CustomName       bool                         `comment:"是否自己改过名称"`
	Power            int64                        `comment:"玩家战斗力"`
	HeadImg          string                       `comment:"玩家头像"`
	AvatarFrame      int32                        `comment:"头像框"`
	Gender           int32                        `comment:"玩家性别"`
	Province         string                       `comment:"国家"`
	City             string                       `comment:"城市"`
	Vip              int32                        `comment:"VIP等级"`
	VipBenefit       map[int32]float64            `comment:"VIP特权加成战斗" ignoreClient:"true"`
	VipBenefitEco    map[int32]float64            `comment:"VIP特权加成科技" ignoreClient:"true"`
	CardTime         map[int32]*GDCard            `comment:"会员卡"`
	RankInfo         *GDRankInfo                  `comment:"段位信息"`
	LordInfo         *GDLordInfo                  `comment:"主攻等级"`
	EvalInfo         *GDEvalInfo                  `comment:"结算信息" ignoreClient:"true"`
	SendMail         *GDSendMail                  `comment:"发送邮件限制" ignoreClient:"true"`
	Items            map[int32]*GDItem            `comment:"拥有物品" redis:"subKey:items"`
	Statistics       map[string]int64             `comment:"数据、统计" redis:"subKey:statistics"`
	StatisticsTime   map[string]int64             `comment:"数据、统计的时间" redis:"subKey:statisticsTime"`
	Heroes           map[int32]*GDHero            `comment:"拥有的武将" redis:"subKey:heroes"`
	GDProducts       map[int32]*GDProduct         `comment:"产出列表" redis:"subKey:products"`
	GDBook           GDBook                       `comment:"图鉴信息"`
	GDBox            GDBox                        `comment:"宝箱"`
	GDMainBattle     GDMainBattle                 `comment:"主线信息"`
	GDRoleTeam       GDRoleTeam                   `comment:"玩法阵容"`
	LastPassDayTime  int64                        `comment:"上一次跨天时间戳"`
	GDShop           GDShop                       `comment:"商城信息"`
	DailyTask        *GDDailyTask                 `comment:"每日任务" redis:"subKey:dailyTask"`
	SPhaseTask       *GDSPhaseTask                `comment:"赛季每期任务" redis:"subKey:sphaseTask"`
	Achievement      map[int32]*GDAchievement     `comment:"成就" redis:"subKey:achievement"`
	MuteTime         int64                        `comment:"禁言时间"`
	Deleted          bool                         `comment:"删除账号" ignoreClient:"true"`
	CreateAt         time.Time                    `comment:"创建时间" mongodb:"index"`
	LoginAt          time.Time                    `comment:"登陆时间"`
	LastLoginAt      time.Time                    `comment:"上次登陆时间"`
	LastOnlineAt     time.Time                    `comment:"最后在线时间"`
	Custom           map[string]int64             `comment:"客户端自定义配置" redis:"subKey:custom"`
	LevelReward      map[int32]bool               `comment:"是否有关卡奖励"`
	StayRwd          *GDStayReward                `comment:"挂机奖励记录"`
	Dungeons         map[int32]int32              `comment:"当前副本进度"`
	RegularGift      *GDRegularGift               `comment:"连续礼包"`
	Guide            map[int32]int32              `comment:"引导记录"`
	GuideStep        int32                        `comment:"引导步骤"`
	MatchRankRwd     map[int32]int32              `comment:"段位奖励0初始,1可领取,2已领取"`
	MatchRankWeekRwd map[int32]int64              `comment:"段位周奖励" ignoreClient:"true"`
	Tower            *GDTower                     `comment:"推塔信息"`
	Tech             *GDTech                      `comment:"主公科技"`
	AttrEco          map[int32]float64            `comment:"科技经济加成"`
	AttrPve          map[int32]float64            `comment:"PVE属性加成" ignoreClient:"true"`
	BossAchiveRwd    map[int32]map[int32]int32    `comment:"boss成就奖励"`
	SoldierBonus     *GDSoldierBonus              `comment:"士兵加成"`
	FixRankRwd       bool                         `comment:"是否修复领取段位奖励" ignoreClient:"true"`
	Fund             map[int32]*GDFund            `comment:"基金奖励领取状态"`
	Dress            map[EMDressType]*GDDress     `comment:"装扮信息"`
	ExpireItemIncId  int64                        `comment:"过期物品自增ID" ignoreClient:"true"`
	ExpireItems      map[int64]*GDExpireItem      `comment:"会过期的物品" redis:"subKey:expireItems"`
	WarState         *GDLWarStateInfo             `comment:"领地战斗状态信息"`
	WarData          *GDLWarData                  `comment:"领地战斗个人数据"`
	SeasonPass       map[int32]*GDSeasonPassInfo  `comment:"赛季信息"`
	HolyBeast        *GDHolyBeastInfo             `comment:"神机兽信息"`
	ServerOpenTime   int64                        `comment:"开服时间"`
	SignInPass       map[string]*GDSignInPassInfo `comment:"签到通行证"`
	Dispatch         *GDDispatch                  `comment:"派遣寻宝" redis:"subKey:dispatch"`
	EnableCarHelp    bool                         `comment:"是否开启车队援助"`
}

// @genData:`value:"true"`
// @comment:`value:"缓存角色"`
type GDCacheRole struct {
	RoleId     int64                    `comment:"角色编号"`
	Name       string                   `comment:"玩家昵称"`
	HeadImg    string                   `comment:"玩家头像"`
	Power      int64                    `comment:"玩家战斗力"`
	MaxPower   int64                    `comment:"玩家最大战斗力"`
	LevelId    int32                    `comment:"段位"`
	Medallion  []int32                  `comment:"徽章"`
	LastOnline int64                    `comment:"最后在线时间"`
	LoginAt    int64                    `comment:"登陆时间"`
	Uid        int64                    `comment:"用户id"`
	ServerId   int64                    `comment:"服务器编号"`
	Dress      map[EMDressType]*GDDress `comment:"装扮信息"`
}

// @genData:`value:"true"`
// @comment:`value:"段位信息"`
type GDRankInfo struct {
	Level         int64 `comment:"段位"`
	LevelScore    int64 `comment:"段位分数"`
	MaxLevelScore int64 `comment:"最大段位分数"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"物品"`
type GDItem struct {
	ItemId   int32       `comment:"物品编号"`
	Quantity int64       `comment:"物品数量"`
	Ext      interface{} `comment:"扩展信息"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"会过期的物品"`
type GDExpireItem struct {
	Id       int64       `comment:"物品编号"`
	ItemId   int32       `comment:"物品编号"`
	Quantity int32       `comment:"物品数量"`
	Ext      interface{} `comment:"扩展信息"`
	Expire   int64       `comment:"过期时间"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @ignoreClient:`value:"true"`
type GDRoleExt struct {
	GDRole
	Ext map[string]any `json:"ext"`
}

// @genData:`value:"true"`
// @comment:`value:"简单角色"`
type GDSimpleRole struct {
	RoleId      int64                  `comment:"角色编号"`
	Name        string                 `comment:"玩家昵称"`
	HeadImg     string                 `comment:"玩家头像"`
	Level       int32                  `comment:"领主等级"`
	Power       int64                  `comment:"战力"`
	LordSkin    int32                  `comment:"装扮主公皮肤"`
	AvatarFrame int32                  `comment:"装扮头像框"`
	Ext         map[string]interface{} `comment:"扩展信息"`
}

// @genData:`value:"true"`
// @comment:`value:"排名用户"`
type GDRankBaseMember struct {
	RoleId int64   `comment:"角色编号"`
	Rank   int32   `comment:"排名"`
	Score  float64 `comment:"排名分数"`
}

// @genData:`value:"true"`
// @comment:`value:"排名用户"`
type GDRankMember struct {
	GDRankBaseMember
	ServerId    int64  `comment:"服务器编号"`
	Name        string `comment:"玩家昵称"`
	HeadImg     string `comment:"玩家头像"`
	Level       int32  `comment:"主线等级"`
	LegionId    int64  `comment:"军团编号"`
	LegionName  string `comment:"军团名称"`
	Power       int64  `comment:"战力"`
	LordLvl     int32  `comment:"主公等级"`
	AvatarFrame int32  `comment:"头像框"`
	LordSkin    int32  `comment:"主公皮肤"`
	PlayerCard  int32  `comment:"vip卡片"`
}

// @genData:`value:"true"`
// @comment:`value:"匹配用户"`
type GDRoomMember struct {
	GDRankBaseMember
	Name           string  `comment:"角色名称"`
	Icon           string  `comment:"角色头像"`
	AvatarFrame    int32   `comment:"头像框"`
	Camp           int     `comment:"阵营"`
	IsAI           bool    `comment:"是否AI"`
	InitGold       int64   `comment:"初始金币" ignoreClient:"true"`
	LordLevel      int32   `comment:"主公等级"`
	SoldierHp      float64 `comment:"士兵生命值"`
	SoldierAtk     float64 `comment:"士兵攻击力"`
	SoldierType    int32   `comment:"士兵类型"`
	SoldierUIType  int32   `comment:"士兵UI类型"`
	Skills         []int32 `comment:"技能id"`
	AIType         int32   `comment:"AI类型" ignoreClient:"true"`
	SoldierId      int32   `comment:"士兵编号"`
	BuySoldierCost int32   `comment:"征兵消耗"`
	BuySoldierEx   int32   `comment:"征兵额外士兵数" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"结算信息"`
type GDEvalInfo struct {
	AddScore            int32           `comment:"结算奖励分"`
	Reward              []*GDRewardData `comment:"结算奖励"`
	ItemMaxGetResetTime int64
	ItemGetPerDayMap    map[int32]int32
}

// @genData:`value:"true"`
// @comment:`value:"图鉴信息"`
type GDBook struct {
	BookPoint      int32 `comment:"图鉴积分"`
	BookLastReward int32 `comment:"积分最后领取的奖励"`
}

// @genData:`value:"true"`
// @comment:`value:"宝箱"`
type GDBox struct {
	IsFirstOpen          bool            `comment:"是否开启低级宝箱"`
	Score                int32           `comment:"宝箱积分"`
	MapUnGetKeyPackCount map[int32]int32 `comment:"宝箱未获关键道具抽取次数"`
	ScoreBoxIndex        int32           `comment:"下一次将领取哪个积分宝箱"`
}

// @genData:`value:"true"`
// @comment:`value:"产出"`
type GDProduct struct {
	Level           int32
	Type            int32
	BaseCollectTime int64
	LastCollectTime int64
	PendingCount    int64
	HeroIds         []int32
}

// @genData:`value:"true"`
// @comment:`value:"主线"`
type GDMainBattle struct {
	Level int32
}

// @genData:`value:"true"`
// @comment:`value:"挂机奖励"`
type GDStayReward struct {
	Recs          map[int32]int64 `comment:"挂机关卡记录"`
	LastClaimTime int64           `comment:"上次领取时间"`
	LastClaimId   int32           `comment:"上次领取关卡id"`
}

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"武器类型"`
type EMEquipPart struct {
	Weapon int32 `enum:"1" comment:"武器"`
	Armor  int32 `enum:"2" comment:"铠甲"`
	Hat    int32 `enum:"3" comment:"头盔"`
	Shoes  int32 `enum:"4" comment:"战靴"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"英雄装备"`
type GDEquip struct {
	EquipSlot EMEquipPart `comment:"装备的英雄槽位"`
	Level     int32       `comment:"装备等级"`
	Attack    float32     `comment:"攻击力"`
	Defense   float32     `comment:"防御力"`
	Hp        float32     `comment:"血量"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"英雄专属装备"`
type GDSpEquip struct {
	Id int32 `comment:"装备编号"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"推塔信息"`
type GDTower struct {
	Id     int32           `comment:"推塔编号"`
	Reward map[int32]int64 `comment:"推塔阶段奖励"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"士兵属性加成"`
type GDSoldierAttr struct {
	Attack float64 `comment:"攻击力加成"`
	Hp     float64 `comment:"血量加成"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"士兵加成"`
type GDSoldierBonus struct {
	Bonus map[int32]GDSoldierAttr `comment:"加成值0,默认,1~4Club"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"基金领取信息"`
type GDFundInfo struct {
	FreeGotten bool `comment:"是否已领取免费基金"`
	PayGotten  bool `comment:"是否已领取付费基金"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"基金信息"`
type GDFund struct {
	Item    map[int32]*GDFundInfo `comment:"领取状态"`
	BuyFlag bool                  `comment:"是否已购买"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"赛季通行证信息"`
type GDSeasonPassInfo struct {
	Phase    string `comment:"期号"`
	Level    int32  `comment:"领取等级"`
	PayLevel int32  `comment:"领取付费等级"`
	BuyFlag  bool   `comment:"是否已购买"`
	EndTime  int64  `comment:"结束时间"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"签到通行证信息"`
type GDSignInPassInfo struct {
	Phase    string `comment:"期号"`
	Level    int32  `comment:"已经领取免费等级"`
	PayLevel int32  `comment:"书费领取付费等级"`
	CanLevel int32  `comment:"当前可领取最高等级"`
	BuyFlag  bool   `comment:"是否已购买"`
	EndTime  int64  `comment:"结束时间"`
	Period   int32  `comment:"期数"`
}

// @genEnum:`value:"true"`
type EMDressType struct {
	AvatarFrame int32 `enum:"1" comment:"头像框"`
	LordSkin    int32 `enum:"6" comment:"主公皮肤"`
	AirShipSkin int32 `enum:"7" comment:"行军皮肤"`
	PlayerCard  int32 `enum:"8" comment:"vip卡片"`
}

// @genData:`value:"true"`
// @comment:`value:"领地争夺战状态信息"`
type GDLWarStateInfo struct {
	Phase         string    `comment:"期号"`
	CurState      EMBFState `comment:"当前状态"`
	NextState     EMBFState `comment:"下一个状态，nil表示无下一个状态"`
	NextStateTime int64     `comment:"下一个状态时间"`
	SeasonDay     int32     `comment:"当前赛季天数"`
	SeasonEndTime int64     `comment:"赛季结束时间"`
	SeasonStart   int64     `comment:"赛季开始时间"`
	NextOpenTime  int64     `comment:"下次开启时间"`
}

// @genData:`value:"true"`
// @comment:`value:"领地争夺战状态信息"`
type GDLWarStateInfoS struct {
	ServerId      int32 `comment:"服务器id"`
	ToServerId    int32 `comment:"合到服务器id"`
	Info          GDLWarStateInfo
	OpenDay       int32 `comment:"开服总天数"`
	AreaRankScore int64 `comment:"服务器的排行榜势力值"`
}

// @genData:`value:"true"`
// @comment:`value:"领地争夺战个人数据"`
type GDLWarData struct {
	RwdCityIds  []int64 `comment:"今日已领取奖励的城市Id"`
	RwdCityTime int64   `comment:"今日城市领取时间"`
}

// @genData:`value:"true"`
// @comment:`value:"神机兽数据"`
type GDHolyBeastInfo struct {
	Id           int32                           `comment:"等级编号"`
	CurStep      int32                           `comment:"当前阶段"`
	CurBonus     map[EHolyBeastAttrIndex]float64 `comment:"当前加成"`
	NextBonus    map[EHolyBeastAttrIndex]float64 `comment:"下一阶段加成"`
	CurExp       int64                           `comment:"当前经验"`
	TotalExp     int64                           `comment:"总经验"`
	IsUnLock     bool                            `comment:"是否解锁"`
	Equips       map[EHBEquipPart]*GDHBEquipInfo `comment:"装备信息"`
	EquipSummary map[int32]float64               `comment:"装备总加成统计"`
}

// @genData:`value:"true"`
// @comment:`value:"神机兽装备数据"`
type GDHBEquipInfo struct {
	Id        int32 `comment:"装备编号"`
	CurExp    int32 `comment:"当前经验(>=8级时用)" ignoreClient:"true"`
	HasBetter bool  `comment:"是否有更好的装备(<8级时用)"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
type RewardDataConfV2 struct {
	// 唯一编号
	Id int32
	// 属性类型
	Type int32
	// 物品编号（hero，item填对应编号；金币钻石不填）
	ItemId int32
	// 权重
	Weight int32
	// 属性值
	Value int64
}

// @genData:`value:"true"`
// @comment:`value:"会员卡"`
type GDCard struct {
	ExpireTime    int64 `comment:"过期时间"`
	LastClaimTime int64 `comment:"最后领取时间"`
}
