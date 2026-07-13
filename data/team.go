package data

// @genEnum:`value:"true"`
// @comment:`value:"阵容类型"`
type TeamType struct {
	Unknown int32 `enum:"-1" comment:"未知"`
	Level   int32 `enum:"0" comment:"主线战斗"`
	Dungeon int32 `enum:"1" comment:"副本战斗"`
	Tower   int32 `enum:"2" comment:"爬塔战斗"`
	Arena   int32 `enum:"3" comment:"竞技场战斗"`
	CarTeam int32 `enum:"4" comment:"赛车战斗"`
}

// @genData:`value:"true"`
// @comment:`value:"角色阵容信息"`
type GDRoleTeam struct {
	RoleId         int64                        `comment:"角色编号"`
	BattleTeamInfo map[TeamType]map[int32]int32 `comment:"阵容"`
	UpdateTimeInfo map[TeamType]int64           `comment:"value是更新时间戳"`
}

// @genEnum:`value:"true"`
// @comment:`value:"阵容类型"`
type ActorType struct {
	Hero         int32 `enum:"0"`
	Monster      int32 `enum:"1"`
	MonsterMerge int32 `enum:"2"`
}
