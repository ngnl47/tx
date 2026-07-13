package data

// @genData:`value:"true"`
// @comment:`value:"战斗数据"`
type BattleData struct {
	Id         int64            `comment:"战斗编号"`
	Mode       int32            `comment:"战斗类型"`
	RandomSeed int64            `comment:"随机种子"`
	Version    int32            `comment:"战斗版本号"`
	MaxRound   int32            `comment:"最大回合数"`
	LeftTeam   *BattleTeam      `comment:"我方"`
	RightTeam  *BattleTeam      `comment:"敌方"`
	Options    map[string]int64 `comment:"自定义参数"`
	OutputCode string           `comment:"结果校验码"`
	Result     *BattleResult    `comment:"战斗结果"`
	FightTime  int64            `comment:"战斗时间"`
	Statistic  []int32          `comment:"统计数据"`
	LeftTeams  []*BattleTeam    `comment:"我方(多人车轮战用)"`
	RightTeams []*BattleTeam    `comment:"敌方(多人车轮战用)"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"战斗队伍"`
type BattleTeam struct {
	RoleId         int64                  `comment:"角色编号"`
	Power          int64                  `comment:"战力"`
	LegionName     string                 `comment:"军团名称"`
	AttributeBonus map[int32]float64      `comment:"属性加成"`
	Team           map[int32]*Fighter     `comment:"队伍"`
	Options        map[string]interface{} `comment:"自定义参数"`
	Name           string                 `comment:"玩家昵称"`
	HeadImg        string                 `comment:"玩家头像"`
	LordLvl        int32                  `comment:"主公等级"`
	BeastLvl       int32                  `comment:"神机兽等级"`
	/*	AvatarFrame         *GDDressItem           `comment:"头像框"`
		CustomCard          *GDDressItem           `comment:"名片"`*/
}

// @genEnum:`value:"true"`
// @comment:`value:"战斗类型"`
type BattleType struct {
	Unknown   int32 `enum:"-1" comment:"未知"`
	Level     int32 `enum:"0" comment:"关卡战斗"`
	Pvp       int32 `enum:"1" comment:"pvp"`
	Dungeon   int32 `enum:"2" comment:"副本战斗"`
	Tower     int32 `enum:"3" comment:"爬塔战斗"`
	Boss      int32 `enum:"4" comment:"蛙王战斗"`
	Duel      int32 `enum:"5" comment:"对决战斗"`
	Arena     int32 `enum:"6" comment:"竞技场战斗"`
	LegionWar int32 `enum:"7" comment:"军团战"`
	Car       int32 `enum:"8" comment:"赛车战"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"武将"`
type Fighter struct {
	Id          int32             `comment:"武将或者怪物编号"`
	Type        int32             `comment:"类型"`
	Index       int32             `comment:"位置"`
	Level       int32             `comment:"等级"`
	Attack      int64             `comment:"攻击"`
	Defense     int64             `comment:"防御"`
	CurHp       int64             `comment:"当前生命"`
	Hp          int64             `comment:"生命"`
	CurEnergy   int64             `comment:"当前怒气"`
	Speed       int64             `comment:"速度"`
	Color       int32             `comment:"品级"`
	Star        int32             `comment:"星级"`
	Skin        int32             `comment:"使用皮肤"`
	SkinName    string            `comment:"皮肤名称"`
	AttackSkill int32             `comment:"普工技能"`
	ActiveSkill int32             `comment:"主动技能"`
	Attribute   map[int32]float64 `comment:"额外属性"`
	Skill       []int32           `comment:"技能"`
	RecordFlag  bool
}

// @genData:`value:"true"`
// @comment:`value:"战斗结果"`
type BattleResult struct {
	Id            int64              `comment:"ID"`
	IsWin         bool               `comment:"是否胜利"`
	Seed          int64              `comment:"随机种子"`
	TotalFrame    int32              `comment:"总帧数"`
	Version       int32              `comment:"回放版本号"`
	BattleVersion string             `comment:"战斗版本号"`
	InputCode     string             `comment:"输入码"`
	OutputCode    string             `comment:"输入码"`
	Log           string             `comment:"战斗log"`
	Sponsor       *TeamInfo          `comment:"我方"`
	Accept        *TeamInfo          `comment:"敌方"`
	Type          int32              `comment:"战斗类型"`
	Round         int32              `comment:"回合数"`
	IsTimeout     int32              `comment:"是否超时"`
	Statistic     map[string][]int64 `comment:"统计数据"`
	Sponsors      []*TeamInfo        `comment:"我方(多人车轮战用)"`
	Accepts       []*TeamInfo        `comment:"敌方(多人车轮战用)"`
	GameResults   [][]int32          `comment:"对战结果(leftIdx,rightIdx,result)"`
}

// @genData:`value:"true"`
type TeamInfo struct {
	RoleId   int64                  `comment:"角色编号"`
	Name     string                 `comment:"角色昵称"`
	HeadImg  string                 `comment:"角色头像"`
	Power    int64                  `comment:"战力"`
	TeamInfo []*TeamMember          `comment:"团队信息"`
	Ext      map[string]interface{} `comment:"扩展信息"`
}

// @genData:`value:"true"`
// @comment:`value:"战斗结果"`
type TeamMember struct {
	HeroId         int32             `comment:"武将或者怪物编号"`
	Color          int32             `comment:"品级"`
	Level          int32             `comment:"星级"`
	Order          int32             `comment:"品阶"`
	Index          int32             `comment:"索引"`
	Rage           int32             `comment:"怒气"`
	Club           int32             `comment:"国家"`
	Slot           int32             `comment:"槽位"`
	Star           int32             `comment:"等级"`
	Damage         int64             `comment:"伤害"`
	TakeDamage     int64             `comment:"承伤"`
	Treatment      int64             `comment:"治疗"`
	Hp             int64             `comment:"生命"`
	Energy         int64             `comment:"怒气"`
	Skin           int32             `comment:"皮肤"`
	SkinName       string            `comment:"皮肤名称"`
	Type           int32             `comment:"类型"`
	MaxAttr        map[int32]float64 `comment:"最大属性"`
	Statistic      map[int32]int64   `comment:"统计数据"`
	SkillDamage    map[int32]int64   `comment:"技能伤害"`
	SkillTreatment map[int32]int64   `comment:"技能治疗"`
}

// @genData:`value:"true"`
type GDBattleHero struct {
	HeroId    int32 `comment:"英雄编号"`
	Level     int32 `comment:"等级"`
	CurHp     int64 `comment:"当前血量"`
	CurEnergy int64 `comment:"当前怒气"`
	TotalHp   int64 `comment:"总血量"`
}
