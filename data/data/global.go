package data

import "time"

// @genEnum:`value:"gcgc,gcvo"`
type EMPlatform struct {
	None       string `enum:"none" comment:"无"`
	WeChatGame string `enum:"wx" comment:"微信小游戏"`
	QQGame     string `enum:"qqgame" comment:"qq小游戏"`
	DYGame     string `enum:"bytedance" comment:"抖音小游戏"`
	Android    string `enum:"android" comment:"安卓"`
	IOS        string `enum:"ios" comment:"ios"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"main" routeBy:"id" idField:"id" collection:"version"`
// @comment:`value:"版本信息"`
type GDVersionInfo struct {
	Id                 string    `comment:"编号" mongodb:"index,unique"`
	Version            string    `comment:"版本号" mongodb:"index:v_p"`
	Platform           string    `comment:"平台类型" mongodb:"index:v_p"`
	BundleVersion      string    `comment:"资源版本号"`
	BundleVersionCache string    `comment:"资源版本号"`
	DataVersion        string    `comment:"数据版本号"`
	DataVersionCache   string    `comment:"数据版本号"`
	ServerUrl          string    `comment:"服务器地址"`
	ResourceUrl        string    `comment:"CDN地址"`
	IsUpgrade          bool      `comment:"是否升级"`
	IsForce            bool      `comment:"是否强制升级"`
	UpdateAnnouncement string    `comment:"更新公告"`
	AppDownloadUrl     string    `comment:"app下载地址"`
	UpdateAt           time.Time `comment:"更新时间"`
	CreateAt           time.Time `comment:"创建时间"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"10" delayDelTime:"-1"`
// @redis:`routeType:"main" routeBy:"platform" idField:"platform" key:"global_config"`
// @mongodb:`routeType:"main" routeBy:"platform" idField:"platform" collection:"global_config"`
// @comment:`value:"全服记录信息"`
type GDGlobalConfig struct {
	Platform           string                      `comment:"平台类型" mongodb:"index"`
	ClientVersion      map[string]*GDClientVersion `comment:"版本信息"`
	ClientVersionCache map[string]*GDClientVersion `comment:"缓存版本信息"`
}

// @genData:`value:"true"`
type GDClientVersion struct {
	Checked    bool      `comment:"是否审核"`
	IsLast     bool      `comment:"是否是最新"`
	IsNew      bool      `comment:"是否是新增"`
	BundleVers string    `comment:"客户端版本信息"`
	ServerUrl  string    `comment:"服务器地址"`
	CreateTime time.Time `comment:"创建时间"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"main" routeBy:"id" idField:"id" collection:"sensitive"`
// @comment:`value:"敏感词替换"`
type GDSensitive struct {
	Id      int64  `comment:"活动编号" mongodb:"index,unique"`
	OldWord string `comment:"老词"`
	NewWord string `comment:"新词"`
	Deleted bool   `comment:"是否删除"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"10" delayDelTime:"86400"`
// @redis:`routeType:"main" routeBy:"uId" idField:"uId" key:"client_version"`
// @mongodb:`routeType:"main" routeBy:"uId" idField:"uId" collection:"client_version"`
// @comment:`value:"客户端版本信息"`
type GDClientVersionData struct {
	UId        string `comment:"用户编号" mongodb:"index"`
	Platform   string `comment:"平台类型" mongodb:"index"`
	Version    string `comment:"版本号"`
	BundleVers string `comment:"客户端版本信息"`
}

// @genEnum:`value:"gcgc,gcvo"`
type EStatisticKey struct {
	DungeonSweep_ZIYUAN    string `enum:"dungeonSweep_ZIYUAN" comment:"副本扫荡-资源"`
	DungeonSweep_YUANBAO   string `enum:"dungeonSweep_YUANBAO" comment:"副本扫荡-元宝"`
	DungeonSweep_BOX       string `enum:"dungeonSweep_BOX" comment:"副本扫荡-宝箱"`
	DungeonStart_ZIYUAN    string `enum:"dungeonStart_ZIYUAN" comment:"副本挑战-资源"`
	DungeonStart_YUANBAO   string `enum:"dungeonStart_YUANBAO" comment:"副本挑战-元宝"`
	DungeonStart_BOX       string `enum:"dungeonStart_BOX" comment:"副本挑战-宝箱"`
	SideGift               string `enum:"sideGift" comment:"侧边礼包"`
	RegularGift            string `enum:"regularGift" comment:"免费礼包"`
	ContinueGiftWeek       string `enum:"regular:continue:update" comment:"连续礼包周"`
	StatisticsLBCount      string `enum:"lb:battle:count" comment:"统计:lb:count"`
	StatisticsLBCountTotal string `enum:"lb:battle:count:total" comment:"统计:lb战斗总次数"`
	DesktopGift            string `enum:"desktopGift" comment:"桌面礼包"`
	BossChCount            string `enum:"bossChCount" comment:"今日boss挑战次数"`
}

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"武器类型"`
type EHolyBeastAttrIndex struct {
	Hp      int32 `enum:"0" comment:"血"`
	Attack  int32 `enum:"1" comment:"攻"`
	Defense int32 `enum:"2" comment:"防"`
}

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"武器类型"`
type EHBEquipPart struct {
	Part1 int32 `enum:"1" comment:"部位1"`
	Part2 int32 `enum:"2" comment:"部位2"`
	Part3 int32 `enum:"3" comment:"部位3"`
	Part4 int32 `enum:"4" comment:"部位4"`
	Part5 int32 `enum:"5" comment:"部位5"`
	Part6 int32 `enum:"6" comment:"部位6"`
}

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"禁言类型"`
type EMMuteType struct {
	Mute_Forever   int32 `enum:"-1" comment:"永久"`
	UnMute         int32 `enum:"0" comment:"解除禁言"`
	Mute_One_Day   int32 `enum:"1" comment:"1天"`
	Mute_Three_Day int32 `enum:"3" comment:"3天"`
	Mute_Five_Day  int32 `enum:"5" comment:"3天"`
	Mute_Seven_Day int32 `enum:"7" comment:"7天"`
}
