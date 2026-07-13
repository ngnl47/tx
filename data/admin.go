package data

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"管理员信息"`
type GDAdminInfo struct {
	Admin      *Admin        `comment:"管理员信息"`
	Menu       []*Permission `comment:"菜单列表"`
	Permission []string      `comment:"权限列表"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"玩家基础信息"`
type GDRoleMiniData struct {
	RoleId       int64                  `comment:"角色编号" mongodb:"index,unique"`
	Uid          int64                  `comment:"用户id" mongodb:"index"`
	Platform     string                 `comment:"平台" mongodb:"index"`
	ServerId     int64                  `comment:"服务器编号" mongodb:"index"`
	AreaId       int64                  `comment:"大区编号" mongodb:"index::areaId:1:levelId:1:loginAt:1:deleted:1:roleId:1"`
	RealServerId int64                  `comment:"真实服务器编号,默认与ServerId相同"`
	ServerName   string                 `comment:"服务器名称"`
	Name         string                 `comment:"玩家昵称" mongodb:"index"`
	HeadImg      string                 `comment:"玩家头像"`
	Gender       int32                  `comment:"玩家性别"`
	Val          int64                  `comment:"资源当前值"`
	Rank         int64                  `comment:"排行"`
	Params       map[string]interface{} `comment:"动态参数"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"资源排行基础信息"`
type ResourceRank struct {
	RoleId int64 `comment:"玩家id"`
	Val    int64 `comment:"资源当前值"`
	Rank   int64 `comment:"排行"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"资源排行基础信息"`
type RoleResRank struct {
	Id            int64          `comment:"查询唯一id"`
	RankCnt       int64          `comment:"排行人数"`
	CheckTime     int64          `comment:"记录查询时间（过了两小时数据重新查询）"`
	UseTime       int64          `comment:"查询耗时（秒）"`
	LastCheckTime int64          `comment:"最后查询时间(用来标记每日自动排行统计数据)"`
	ServerId      []int64        `comment:"服务器 id 列表"`
	ResRankItem   []*ResRankItem `comment:"资源数据"`
	State         bool           `comment:"是否查询完成（false 未完成 true 完成）"`
	Force         bool           `comment:"是否强置查询"`
	AutoCheck     bool           `comment:"每日自动查询数据"`
	Desc          string         `comment:"查询备注信息"`
	Close         bool           `comment:"关闭查询"`
	Process       int64          `comment:"进度"`
}

// @genData:`value:"true"`
// @storage:`mode:"redis"`
// @redis:`routeType:"resRank" routeBy:"id" idField:"id" key:"res_rank_check_detail"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"资源排行基础信息"`
type ResRankItem struct {
	Id            string                  `comment:"资源唯一id"`
	ResType       int64                   `comment:"资源类型"`
	ResId         int64                   `comment:"资源编号"`
	ResName       string                  `comment:"资源名字"`
	NumberOneInfo *GDRoleMiniData         `comment:"第一名信息"`
	RankSnapData  map[int64]*ResourceRank `comment:"排行简讯数据"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"redis"`
// @redis:`routeType:"resRank" routeBy:"id" idField:"id" key:"res_rank_check_data"`
// @comment:`value:"资源排行基础信息"`
type ResourceRankDetail struct {
	Id          int32          `comment:"id"`
	RoleResRank []*RoleResRank `comment:"查询详情"`
}
