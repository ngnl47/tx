package data

import "time"

// @genData:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"main" routeBy:"areaId" idField:"areaId" collection:"areas" enableSharding:"true"`
// @comment:`value:"服务器大区"`
type GDArea struct {
	AreaId      int64          `comment:"大区原始编号" mongodb:"index,unique"`
	Platform    string         `comment:"平台" mongodb:"index:pa_id"`
	ViewId      int64          `comment:"大区显示编号" mongodb:"index:pa_id"`
	AreaName    string         `comment:"大区名称"`
	ServerCount map[int64]bool `comment:"服务器数量"`
	UpdateTime  time.Time      `comment:"修改时间"`
	CreateTime  time.Time      `comment:"创建时间"`
}

// @genData:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"main" routeBy:"id" idField:"id" collection:"servers" enableSharding:"true"`
// @customExport:`value:"true"`
// @comment:`value:"服务器"`
type GDServer struct {
	Id          int64     `comment:"服务器原始编号" mongodb:"index,unique"`
	Platform    string    `comment:"平台" mongodb:"index:ps_id"`
	PlatformExt string    `comment:"平台扩展"`
	ViewId      int64     `comment:"服务器显示编号" mongodb:"index:ps_id"`
	RealId      int64     `comment:"服务器真实编号" mongodb:"index"`
	AreaId      int64     `comment:"大区编号" mongodb:"index"`
	State       int32     `comment:"服务器状态:0:停服、1:正常、2:尚未开启"`
	OpenTime    time.Time `comment:"如果为停服或未开启状态，则为开启时间"`
	Version     string    `comment:"服务器版本"`
	ServerName  string    `comment:"服务器名称"`
	UpdateTime  time.Time `comment:"修改时间"`
	CreateTime  time.Time `comment:"创建时间"`
}

// @genData:`value:"true"`
// @comment:`value:"服务器排名（如服级控制区汇总榜，条目 Id/ServerId 为 RealServerId）"`
type GDRankServer struct {
	Id         int64   `comment:"服务器 RealServerId"`
	Name       string  `comment:"服务器名称"`
	ServerId   int64   `comment:"服务器编号（RealServerId）"`
	Rank       int32   `comment:"排名"`
	Score      float64 `comment:"排名分数"`
	UpdateTime int64   `comment:"更新时间" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"服务器 merge 节点"`
type GDMergeNode struct {
	ID         int64  `comment:"id" json:"id"`
	Key        int64  `comment:"原值" json:"k"`
	Value      int64  `comment:"映射值" json:"v"`
	Comment    string `comment:"说明" json:"cmt"`
	CreateTime int64  `comment:"创建时间戳(ms)" json:"ct"`
	EffectTime int64  `comment:"生效时间戳(ms)" json:"et"`
}

// @genData:`value:"true"`
// @comment:`value:"军团战的甘特图任务块"`
type GDWarGanttNode struct {
	ID         int64              `comment:"id" json:"id"`
	Type       EMWarGanttNodeType `comment:"任务块类型" json:"type"`
	Key        int64              `comment:"原值" json:"k"`
	Value      int64              `comment:"映射值" json:"v"`
	Comment    string             `comment:"说明" json:"cmt"`
	StartTime  int64              `comment:"开始时间戳(ms)" json:"st"`
	EndTime    int64              `comment:"结束时间戳(ms)" json:"et"`
	CreateTime int64              `comment:"创建时间戳(ms)" json:"ct"`
	MServerId  int64              `comment:"大于0表示是从服,被合入的主服id" json:"mSid"`
}

// @genEnum:`value:"true"`
// @comment:`value:"任务块类型"`
type EMWarGanttNodeType struct {
	Battle int `comment:"活动期" enum:"0"`
	Stop   int `comment:"暂停期" enum:"1"`
}
