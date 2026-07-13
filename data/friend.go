package data

import "time"

// @genData:`value:"true"`
// @storage:`mode:"mongo" delaySaveTime:"60" delayDelTime:"600"`
// @redis:`routeType:"main" routeBy:"roleId" idField:"roleId" key:"friend"`
// @mongodb:`routeType:"main" routeBy:"roleId" idField:"roleId" collection:"friends" enableSharding:"true" useMongoObjectID:"true"`
// @comment:`value:"好友记录"`
type GDFriendRecord struct {
	RoleId     int64               `comment:"角色编号" mongodb:"index,unique"`
	Friends    map[int64]*GDFriend `comment:"好友列表"`
	CreateTime time.Time           `comment:"创建时间" ignoreClient:"true"`
	UpdateTime time.Time           `comment:"修改时间" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"好友信息"`
type GDFriend struct {
	RoleId     int64     `comment:"好友编号"`
	ServerId   int64     `comment:"服务器编号"`
	Name       string    `comment:"玩家昵称"`
	HeadImg    string    `comment:"玩家头像"`
	Level      int32     `comment:"段位"`
	Medallion  []int32   `comment:"徽章"`
	IsOnline   bool      `comment:"是否在线"`
	IsReceive  bool      `comment:"是否可以收礼"`
	Received   int64     `comment:"领取时间"`
	Presented  int64     `comment:"赠送时间"`
	LastLogin  int64     `comment:"最后登陆时间"`
	CreateTime time.Time `comment:"创建时间" ignoreClient:"true"`
	UpdateTime time.Time `comment:"修改时间" ignoreClient:"true"`
}

// @genData:`value:"true"`
// @comment:`value:"好友扩展信息"`
type GDFriendExt struct {
	RoleId int64 `comment:"好友编号"`
}
