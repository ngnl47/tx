package data

// @genEnum:`value:"true"`
// @comment:`value:"支付SDK创建账号类型"`
type EMCreateRoleType struct {
	OldUser         int32 `enum:"0" comment:"老角色"`
	FirstRoleCreate int32 `enum:"1" comment:"创建第一个角色"`
	RoleCreate      int32 `enum:"2" comment:"角色创建"`
}

// @genData:`value:"true"`
// @comment:`value:"装扮信息（头像框/名片/飞艇/连杀）"`
type GDDress struct {
	Typ     EMDressType            `comment:"类型"`
	Used    int32                  `comment:"已穿戴"`
	Storage map[int32]*GDDressItem `comment:"装扮仓库"`
}

// @genData:`value:"true"`
// @comment:`value:"装扮具体信息（头像框/名片/飞艇/连杀）"`
type GDDressItem struct {
	Id     int32 `comment:"编号"`
	Expire int64 `comment:"过期时间（-1表示永久）"`
}

// @genData:`value:"true"`
// @mongodb:`collection:"record_arena" idField:"id" routeType:"role" routeBy:"roleId" enableSharding:"true"`
// @storage:`mode:"mongo"`
// @comment:`value:"竞技场战报"`
type GDArenaRecord struct {
	Id              int64        `comment:"id" mongodb:"index"`
	RoleId          int64        `comment:"自己角色编号" mongodb:"index"`
	OppositeId      int64        `comment:"对方角色编号"`
	OppositeName    string       `comment:"对方昵称"`
	OppositeHead    string       `comment:"对方头像"`
	OppoAvatarFrame *GDDressItem `comment:"头像框"`
	IsAttack        bool         `comment:"是否进攻"`
	IsWin           bool         `comment:"是否胜利"`
	Score           int32        `comment:"分数的变化"`
	Result          string       `comment:"对战详情"`
	Created         int64        `comment:"创建时间"`
	LastWin         bool         `comment:"最后一场是否胜利, 不管谁打谁"`
	BattleId        int64        `comment:"战斗id"`
	OppoPower       int64        `comment:"对方战斗力"`
	ServerId        int64        `comment:"已方服务器id"`
	OppoServerId    int64        `comment:"对方服务器id"`
}
