package data

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"3600" delayDelTime:"86400"`
// @redis:`routeType:"role" routeBy:"roleId" idField:"roleId" key:"roleCarCar"`
// @mongodb:`routeType:"role" routeBy:"roleId" idField:"roleId" collection:"roleCarCar" enableSharding:"true"`
// @customExport:`value:"true"`
// @comment:`value:"玩家赛车信息"`
type GDRoleCar struct {
	RoleId                  int64                 `comment:"角色编号" mongodb:"index,unique" ignoreClient:"true"`
	CarMap                  map[int]string        `comment:"玩家发车信息（key是slot，0-3）" ignoreClient:"true"`
	CarDataMap              map[string]*GDCar     `comment:"玩家发车信息（key是carId）"`
	FreeRaidCnt             int32                 `comment:"免费掠夺次数"`
	RaidCntResetTime        int64                 `comment:"掠夺次数重置时间"`
	RaidMap                 map[string]*CarRaid   `comment:"掠夺次数（key是carId）"`
	SuccessRaidCnt          int32                 `comment:"成功掠夺次数"`
	SuccessRaidCntResetTime int64                 `comment:"成功掠夺次数重置时间"`
	SendCnt                 int32                 `comment:"发车次数"`
	SendCntResetTime        int64                 `comment:"发车次数重置时间"`
	InvitedHelpCnt          int32                 `comment:"今日请求协助次数"`
	RecommendTime           int64                 `comment:"推荐列表刷新时间"`
	RecommendLegionId       int64                 `comment:"推荐的军团" ignoreClient:"true"`
	Recommends              map[string]*GDCarInfo `comment:"推荐列表"`
	WantedMap               map[int64]int64       `comment:"已通缉的人（key是roleId，val是结束时间戳）"`
	BeWantedMap             map[int64]int64       `comment:"我被谁通缉（key是roleId，val是结束时间戳）" ignoreClient:"true"`
	BeWantedRaidSuccCnt     map[int64]int32       `comment:"被通缉方侧：各通缉方对其成功掠夺次数（达次数清除通缉）" ignoreClient:"true"`
	WeekPoints              int64                 `comment:"本周航海积分"`
	WeekPointsResetTime     int64                 `comment:"本周航海积分重置时间"`
	Phase                   int32                 `comment:"期次"`
	SentOutHeroMap          map[int32]int32       `comment:"已发出的英雄,0代表未出发,1代表在1队"`
	HelpOtherTeam           *BattleTeam           `comment:"协助他人队伍" redis:"subKey:helpOtherTeam"`
	SendHelpTime            int64                 `comment:"开关协助时间" ignoreClient:"true"`
	IsMergedPool            bool                  `comment:"是否是合并的池子"`
	IsShowCurServer         bool                  `comment:"是否刷本服的船只"`
}

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"3600" delayDelTime:"21600"`
// @redis:`routeType:"teamMatch" routeBy:"id" idField:"id" key:"carCarData"`
// @mongodb:`routeType:"role" routeBy:"id" idField:"id" collection:"carCarData" enableSharding:"true" useMongoObjectID:"true"`
// @customExport:`value:"true"`
// @comment:`value:"赛车数据"`
type GDCar struct {
	Id               string          `comment:"编号" mongodb:"index,unique"`
	RoleId           int64           `comment:"角色编号"`
	LegionId         int64           `comment:"军团编号"`
	PoolId           int64           `comment:"池子编号" ignoreClient:"true"`
	GroupId          int64           `comment:"小组编号" ignoreClient:"true"`
	PoolDeleted      bool            `comment:"是否从池子删除" ignoreClient:"true"`
	GroupDeleted     bool            `comment:"是否从小组池子删除" ignoreClient:"true"`
	Slot             int             `comment:"车位0-3"`
	ServerId         int64           `comment:"服务器编号"`
	HelperServerId   int64           `comment:"助战服务器编号"`
	Color            int32           `comment:"品质"`
	CreateAt         int64           `comment:"生成时间" ignoreClient:"true"`
	SendAt           int64           `comment:"发出时间"`
	Duration         int64           `comment:"持续时间"`
	ClaimAt          int64           `comment:"领取时间"`
	RefreshCount     int32           `comment:"刷新次数"`
	Rewards          []*GDRewardData `comment:"奖励列表"`
	Raided           int32           `comment:"被掠夺次数"`
	BattleTeam       *BattleTeam     `comment:"我的战斗队伍" redis:"subKey:carBattleTeam"`
	HelperBattleTeam *BattleTeam     `comment:"护送者战斗队伍" redis:"subKey:carHelperBattleTeam"`
	HelperId         int64           `comment:"助战者编号"`
	Name             string          `comment:"玩家昵称" ignoreClient:"true"`
	HeadImg          string          `comment:"玩家头像" ignoreClient:"true"`
	AvatarFrame      *GDDressItem    `comment:"头像框" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"掠夺信息"`
type CarRaid struct {
	Cnt      int32 `comment:"成功掠夺次数"`
	TotalCnt int32 `comment:"总掠夺次数"`
	SendAt   int64 `comment:"发车时间" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"通用角色信息"`
type GDRoleInfo struct {
	RoleId      int64        `comment:"玩家编号"`
	ServerId    int64        `comment:"服务器编号"`
	Name        string       `comment:"玩家昵称"`
	HeadImg     string       `comment:"玩家头像"`
	AvatarFrame *GDDressItem `comment:"头像框"`
	CustomCard  *GDDressItem `comment:"名片"`
	Power       int64        `comment:"战力"`
	LegionId    int64        `comment:"军团编号"`
	LegionName  string       `comment:"军团名称"`
}

// @genData:`value:"true"`
// @comment:`value:"通用角色信息"`
type GDHelperRoleInfo struct {
	GDRoleInfo
	Rank        int32       `comment:"排名" ignoreClient:"true"`
	Score       float64     `comment:"排名分数" ignoreClient:"true"`
	BattleTeam  *BattleTeam `comment:"协助队伍"`
	NextEndTime int64       `comment:"发车结束时间0为未协助"`
	HelpCnt     int32       `comment:"助战次数"`
}

// @genData:`value:"true"`
// @comment:`value:"车辆信息"`
type GDCarInfo struct {
	Id          string       `comment:"车辆编号"`
	Color       int32        `comment:"品质"`
	SendAt      int64        `comment:"发出时间"`
	IsWanted    bool         `comment:"是否通缉"`
	Duration    int64        `comment:"持续时间"`
	Raided      int32        `comment:"被掠夺次数"`
	HelperId    int64        `comment:"助战者编号"`
	Name        string       `comment:"车主名称"`
	HeadImg     string       `comment:"车主头像"`
	AvatarFrame *GDDressItem `comment:"车主头像框"`
	Power       int64        `comment:"车主战力"`
	LegionId    int64        `comment:"车主军团编号"`
	ServerId    int64        `comment:"服务器编号" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"历史记录"`
type GDCarLog struct {
	Id             string          `comment:"记录编号"`
	Car            *GDCar          `comment:"车辆信息"`
	RoleInfo       *GDRoleInfo     `comment:"车主信息"`
	Rewards        []*GDRewardData `comment:"奖励列表"`
	Points         int32           `comment:"获得积分"`
	CreateAt       int64           `comment:"创建时间(如果是护卫记录，是收车时间)"`
	Result         EMCarLogResult  `comment:"结果"`
	BattlePlayback string          `comment:"战斗回放记录oss文件名"`
	AttackTeam     *BattleTeam     `comment:"进攻队伍"`
}

// @genEnum:`value:"true"`
type EMCarLogResult struct {
	RaidWin     int32 `enum:"1" comment:"掠夺成功"`
	RaidLose    int32 `enum:"2" comment:"掠夺失败"`
	DefenseWin  int32 `enum:"3" comment:"防守成功"`
	DefenseLose int32 `enum:"4" comment:"防守失败"`
}

// @genEnum:`value:"true"`
type EMCarLogType struct {
	Battle int32 `enum:"1" comment:"战斗"`
	Claim  int32 `enum:"2" comment:"收车"`
	Help   int32 `enum:"3" comment:"护送"`
}
