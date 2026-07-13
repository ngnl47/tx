package data

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"600"`
// @redis:`routeType:"role" routeBy:"roleId" idField:"roleId" key:"area_arena"`
// @mongodb:`routeType:"role" routeBy:"roleId" idField:"roleId" collection:"area_arena" enableSharding:"true" useMongoObjectID:"true"`
// @comment:`value:"跨服竞技场"`
type GDAreaArena struct {
	RoleId               int64                            `comment:"角色编号" mongodb:"index,unique"`
	Level                int32                            `comment:"当前段位"`
	PhaseInfo            map[string]*GDAreaArenaPhaseInfo `comment:"期号信息(Key是期号,格式:060102))"`
	DailySettlementStamp int64                            `comment:"日结算时间戳"`
	WeekSettlementStamp  int64                            `comment:"周结算时间戳"`
	CreateTime           int64                            `comment:"创建时间"`
}

// @genData:`value:"true"`
// @comment:`value:"跨服竞技场期次信息"`
type GDAreaArenaPhaseInfo struct {
	Phase      string  `comment:"期号(格式:060102)"`
	SeasonIdx  int     `comment:"赛季"`
	AreaId     int64   `comment:"区"`
	WeekRank   int64   `comment:"周排名"`
	GroupId    int64   `comment:"分组编号"`
	Level      int32   `comment:"段位"`
	Score      float64 `comment:"分数"`
	ServerList []int32 `comment:"合服列表,空表示没有合服"`
	CreateTime int64   `comment:"创建时间"`
}

// @genEnum:`value:"true"`
type EMAreaArenaRankType struct {
	Default int32 `enum:"0" comment:"默认榜"`
}
