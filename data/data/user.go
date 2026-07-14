package data

import "time"

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"120" delayDelTime:"86400"`
// @redis:`routeType:"user" routeBy:"key" idField:"key" key:"user"`
// @mongodb:`routeType:"user" routeBy:"key" idField:"key" collection:"users" enableSharding:"true"`
// @comment:`value:"用户"`
type GDUser struct {
	Id               int64                   `comment:"用户id" mongodb:"index"`
	Key              string                  `comment:"用于查询的唯一key" mongodb:"index,unique"`
	Platform         string                  `comment:"平台" mongodb:"index:p_id"`
	PlatformExt      string                  `comment:"平台扩展"`
	PlatformUId      string                  `comment:"平台用户id" mongodb:"index:p_id,index"`
	PlatformId       int64                   `comment:"平台ID"`
	OpenId           string                  `comment:"微信小游戏 openId" mongodb:"index"`
	UnionId          string                  `comment:"微信 unionID" mongodb:"index"`
	UserName         string                  `comment:"用户信息"`
	UserImg          string                  `comment:"头像"`
	UserSex          int32                   `comment:"性别， 0：未知，1：男，2：女"`
	ShareCode        string                  `comment:"分享码" mongodb:"index"`
	LoginPlatform    string                  `comment:"登录的平台"`
	LoginType        string                  `comment:"登录方式"`
	Province         string                  `comment:"省"`
	City             string                  `comment:"市"`
	IsSubscribe      bool                    `comment:"是否显示关注"`
	Authed           bool                    `comment:"是否授权"`
	Channel          string                  `comment:"所属渠道"`
	Scene            int32                   `comment:"微信场景"`
	ReferrerInfo     string                  `comment:"微信来源"`
	IsBan            bool                    `comment:"是否封禁"`
	LastLoginIP      string                  `comment:"最后登陆IP"`
	Roles            map[int64]*GDServerRole `comment:"角色编号"`
	LastServerId     int64                   `comment:"最后登陆服务器编号"`
	Ban              *GDBanInfo              `comment:"封禁信息"`
	PlatformInfos    map[string]interface{}  `comment:"平台信息LoginFrom=>Info"`
	MorePlatformUIds []string                `comment:"关联的平台用户id"`
	RegTime          time.Time               `comment:"注册时间"`
	DeleteTime       int64                   `comment:"注销时间"`
}

// @genData:`value:"true"`
type GDServerRole struct {
	Id       int64     `comment:"角色id"`
	ServerId int64     `comment:"服务器id"`
	Name     string    `comment:"角色名"`
	Level    int32     `comment:"主公等级"`
	Power    int64     `comment:"角色战斗力"`
	HeadImg  string    `comment:"角色头像"`
	LoginAt  time.Time `comment:"上次登录时间"`
}

// @genEnum:`value:"gcgc"`
type EMSex struct {
	Unknown int `enum:"0"`
	Male    int `enum:"1"`
	Female  int `enum:"2"`
}

// @genData:`value:"true"`
type GDBanInfo struct {
	Reason  string    `comment:"封禁原因"`
	EndTime time.Time `comment:"封禁解除时间"`
}
