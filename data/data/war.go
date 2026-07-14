package data

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"3600" delayDelTime:"18000" memoryCache:"1200"`
// @redis:`routeType:"battlefield" routeBy:"id" idField:"id" key:"battlefields" omitempty:"true"`
// @mongodb:`routeType:"battlefield" routeBy:"id" idField:"id" collection:"battlefields" enableSharding:"true" useMongoObjectID:"true"`
// @comment:`value:"战场数据"`
type GDBattlefield struct {
	Id              string                `comment:"战场id" mongodb:"index"`
	IsViewStore     bool                  `comment:"是否是视野store" ignoreClient:"true"`
	WorldId         string                `comment:"世界id"`
	GridX           int                   `comment:"网格X"`
	GridY           int                   `comment:"网格Y"`
	RealServerId    int64                 `comment:"服务器编号"`
	State           EMBFState             `comment:"战场状态"`
	AutoIncId       int64                 `comment:"自动递增ID" ignoreClient:"true"`
	NextStateTime   int64                 `comment:"下次状态变更时间"`
	SeasonEndTime   int64                 `comment:"赛季结束时间"`
	SeasonStartTime int64                 `comment:"赛季开始时间"`
	BuildMap        map[int64]*GDBuilding `comment:"建筑物列表"`
	Legions         map[int64]*GDBFLegion `comment:"军团列表"`
	Roles           map[int64]*GDBFRole   `comment:"成员列表"`
	Marches         map[int64]*GDMarch    `comment:"行军列表"`
	BattleMap       map[int64]*GDBFBattle `comment:"战斗列表"`
}

// @genEnum:`value:"true"`
type EMBFState struct {
	Subs    string `enum:"subs" comment:"宣战"`
	Ready   string `enum:"ready" comment:"准备"`
	Started string `enum:"started" comment:"攻城"`
	Ended   string `enum:"ended" comment:"休战"`
}

// @genData:`value:"true"`
type GDBuilding struct {
	Id         int64           `comment:"建筑编号"`                     //:"id"`
	Type       int             `comment:"建筑类型"`                     //:"T"`
	MaxHP      int32           `comment:"建筑最大血量"`                   //:"MHP"`
	HP         int32           `comment:"建筑血量"`                     //:"H"`
	Point      int32           `comment:"建筑积分" ignoreClient:"true"` //:"P"`
	BattleHp   int32           `comment:"战斗血量" ignoreClient:"true"` //:"BH"`
	Pos        int64           `comment:"建筑位置" ignoreClient:"true"` //:"Pos"`
	BelongLId  int64           `comment:"归属军团"`                     //:"LId"`
	SubsLIdMap map[int64]int32 `comment:"约战军团id-积分"`                //:"SLIM"`
	FirstOccu  bool            `comment:"是否首次占领过"`                  //:"FO"`
}

// @genData:`value:"true"`
// @comment:`value:"坐标信息"`
type GDPoint struct {
	X int32 `comment:"坐标X"`
	Y int32 `comment:"坐标Y"`
}

// @genEnum:`value:"true"`
type EMLegionState struct {
	Normal string `enum:"normal" comment:"正常"`
	Out    string `enum:"out" comment:"出局"`
}

// @genData:`value:"true"`
type GDBFLegion struct {
	Id       int64         `comment:"军团编号"`   //:"id"`
	Name     string        `comment:"军团名称"`   //:"N"`
	Logo     string        `comment:"军团logo"` //:"LG"`
	Power    int64         `comment:"军团战力"`   //:"P"`
	Group    int64         `comment:"分组编号"`   //:"G"`
	ServerId int64         `comment:"服务器编号"`  //:"SId"`
	State    EMLegionState `comment:"军团状态"`   //:"S"`
	Point    int64         `comment:"军团积分"`   //:"PT"`
	Level    int32         `comment:"军团等级"`   //:"LV"`
	Color    int32         `comment:"军团颜色"`   //:"CO"`
}

// @genEnum:`value:"true"`
type EMRoleState struct {
	Watching string `enum:"W" comment:"观战"`
	Idle     string `enum:"I" comment:"待机"`
	March    string `enum:"M" comment:"行军"`
	Combat   string `enum:"C" comment:"战斗"`
	Die      string `enum:"D" comment:"死亡"`
	Over     string `enum:"O" comment:"结束"`
	Sleep    string `enum:"S" comment:"休息"`
}

// @genEnum:`value:"true"`
type EMRoleType struct {
	Guest  int32 `enum:"0" comment:"观战游客"`
	Player int32 `enum:"1" comment:"参战玩家"`
}

// @genData:`value:"true"`
type GDBFRole struct {
	Id                  int64       `comment:"玩家编号"`                    //:"id"`
	LegionId            int64       `comment:"军团编号"`                    //:"LId"`
	State               EMRoleState `comment:"玩家状态"`                    //:"ST"`
	Type                EMRoleType  `comment:"玩家类型"`                    //:"T"`
	BuildId             int64       `comment:"建筑编号"`                    //:"BId"`
	Point               int64       `comment:"积分"`                      //:"PT"`
	Energy              int64       `comment:"玩家能量"`                    //:"EG"`
	Diamond             int64       `comment:"钻石" ignoreClient:"true"`  //:"D"`
	ReviveItem          int32       `comment:"复活丹" ignoreClient:"true"` //:"RI"`
	Revive              int32       `comment:"复活次数"`                    //:"R"`
	ReviveTime          int64       `comment:"复活时间"`                    //:"RT"`
	DieTime             int64       `comment:"死亡时间"`                    //:"DT"`
	Power               int64       `comment:"玩家战力"`                    //:"P"`
	ConKill             int32       `comment:"连杀次数"`                    //:"CK"`
	LastKillRoleId      int64       `comment:"击败的连杀玩家"`                 //:"LKR"`
	LastKillRoleConKill int32       `comment:"击败的连杀玩家累计击杀数"`            //:"LKRCK"`
	AirShipSkinId       int32       `comment:"行军皮肤"`                    //:"ASSId"`
}

// @genData:`value:"true"`
type GDMarch struct {
	Id        int64 `comment:"行军编号"` //:"id"`
	RoleId    int64 `comment:"玩家编号"` //:"RId"`
	FromBId   int64 `comment:"起始建筑"` //:"FBId"`
	ToBId     int64 `comment:"目标建筑"` //:"TBId"`
	EBId      int64 `comment:"最终建筑"` //:"EBId"`
	StartTime int64 `comment:"开始时间"` //:"ST"`
	EndTime   int64 `comment:"结束时间"` //:"ET"`
}

// @genEnum:`value:"true"`
type EMBattleType struct {
	PVP int32 `enum:"1" comment:"玩家对玩家"`
	PVB int32 `enum:"2" comment:"玩家对建筑"`
}

// @genData:`value:"true"`
type GDBFBattle struct {
	Id          int64        `comment:"战斗编号"`   //:"id"`
	BattleType  EMBattleType `comment:"战斗类型"`   //:"BT"`
	LeftRoleId  int64        `comment:"左侧玩家"`   //:"LId"`
	RightRoleId int64        `comment:"右侧玩家"`   //:"RId"`
	BuildId     int64        `comment:"目标建筑"`   //:"BId"`
	WinFlag     EMWinFlag    `comment:"战斗胜利标记"` //:"WF"`
	StartTime   int64        `comment:"战斗开始时间"` //:"ST"`
	EndTime     int64        `comment:"战斗结束时间"` //:"ET"`
}

// @genData:`value:"true"`
type GDBFHero struct {
	HeroId     int32 `comment:"英雄编号"`
	OriAtk     int64 `comment:"原始攻击力"`
	OriDefense int64 `comment:"原始防御力"`
	OriHp      int64 `comment:"原始血量"`
	CurHp      int64 `comment:"当前血量"`
	CurEnergy  int64 `comment:"当前怒气"`
	Star       int32 `comment:"英雄星级"`
	Level      int32 `comment:"英雄等级"`
}

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"战斗胜利标记"`
type EMWinFlag struct {
	Default int32 `enum:"0" comment:"默认"`
	Lose    int32 `enum:"1" comment:"输"`
	Win     int32 `enum:"2" comment:"赢"`
	Draw    int32 `enum:"3" comment:"平局"`
}

// @genData:`value:"true"`
type GDBFRoleInfo struct {
	Id         int64  `comment:"玩家编号"`                     //:"id"`
	LegionId   int64  `comment:"军团编号"`                     //:"LId"`
	Name       string `comment:"玩家名称"`                     //:"N"`
	HeadImg    string `comment:"玩家头像"`                     //:"HI"`
	Power      int64  `comment:"玩家战力" ignoreClient:"true"` //:"P"`
	LegionName string `comment:"军团名称"`
	ServerId   int64  `comment:"服务器id"`
}

// @genData:`value:"true"`
type GDBFLegionInfo struct {
	Id   int64  `comment:"军团编号"` //:"id"`
	Name string `comment:"军团名称"` //:"N"`
	Logo string `comment:"军团头像"` //:"HI"`
}

// @genData:`value:"true"`
type GDLegionWarInfo struct {
	HadSignUp        bool    `comment:"是否已报名"`
	BirthBuildId     int64   `comment:"出生建筑id"`
	SubsBuildIds     []int64 `comment:"建筑id列表"`
	SubsCnt          int32   `comment:"约战次数"`
	CanSubsBIdList   []int64 `comment:"可约战建筑id列表"`
	OccupyBIdList    []int64 `comment:"占领建筑id列表"`
	OriOccupyBIdList []int64 `comment:"今日原始占领建筑id列表"`
	SId              string  `comment:"服务器id"`
}

// @genData:`value:"true"`
type GDBattle struct {
	Id              int64         `comment:"战斗编号"`
	BattleType      EMBattleType  `comment:"战斗类型"`
	BuildingId      string        `comment:"建筑编号"`
	BattleData      *BattleData   `comment:"战斗数据"`
	LeftId          int64         `comment:"进攻玩家编号"`
	RightId         int64         `comment:"防守玩家编号"`
	LeftLastEnergy  int64         `comment:"左边玩家上次精力值" ignoreClient:"true"`
	RightLastEnergy int64         `comment:"右边玩家上次精力值" ignoreClient:"true"`
	StartTime       int64         `comment:"开始时间"`
	EndTime         int64         `comment:"结束时间"`
	IsWin           bool          `comment:"是否胜利"`
	Result          *BattleResult `comment:"战斗结果"`
	TeamMemberCnt   int           `comment:"队伍人数"`
}

// @genData:`value:"true"`
// @comment:`value:"军团战战报信息"`
type GDLWBattleDetails struct {
	RoleId         int64                 `comment:"玩家id"`
	Name           string                `comment:"玩家昵称"`
	HeadImg        string                `comment:"玩家头像"`
	AvatarFrame    int32                 `comment:"头像框"`
	Power          int64                 `comment:"战力"`
	BuildingCnt    int64                 `comment:"攻城次数"`
	WinCnt         int64                 `comment:"胜利次数"`
	LoseCnt        int64                 `comment:"失败次数"`
	TargetRoleList []*GDLWTargetRoleBase `comment:"对手信息"`
}

// @genData:`value:"true"`
// @comment:`value:"军团战战报对手用户信息"`
type GDLWTargetRoleBase struct {
	RoleInfo       *GDLWRoleBase  `comment:"玩家id"`
	TargetRoleInfo *GDLWRoleBase  `comment:"玩家id"`
	AttackType     GDLMAttackType `comment:"进攻方式"`
	WinFlag        bool           `comment:"胜利标记"`
	RecordIsOss    bool           `comment:"是否采用oss存储"`
	RecordId       string         `comment:"战斗记录"`
	OssFileName    string         `comment:"oss文件记录"`
	Position       GDPoint        `comment:"玩家坐标"`
	Timestamp      int64          `comment:"时间戳"`
	NewWinFlag     EMWinFlag      `comment:"新的胜利标记"`
}

// @genData:`value:"true"`
// @comment:`value:"军团战战报对手用户信息"`
type GDLWRoleBase struct {
	RoleId      int64  `comment:"玩家id"`
	Name        string `comment:"玩家昵称"`
	HeadImg     string `comment:"玩家头像"`
	Power       int64  `comment:"战力"`
	CodeId      int32  `comment:"玩家代号编号"`
	AvatarFrame int32  `comment:"头像框"`
}

// @genEnum:`value:"true"`
// @comment:`value:"军团排位赛成员信息"`
type GDLMAttackType struct {
	Attack  int32 `enum:"0" comment:"第一人进攻"`
	Defense int32 `enum:"1" comment:"第一人防守"`
}

// @genData:`value:"true"`
// @storage:`mode:"redis" delayDelTime:"2592000"`
// @redis:`routeType:"role" routeBy:"uId" idField:"uId" key:"legion_war_record"`
// @comment:`value:"军团战战斗记录"`
type GDLegionWarRecord struct {
	UId        string    `comment:"唯一ID"`
	BattleData *GDBattle `comment:"战斗数据"`
}
