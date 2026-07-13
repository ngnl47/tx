package data

// @genData:`value:"true"`
// @comment:`value:"主攻等级"`
type GDLordInfo struct {
	Level            int32          `comment:"主公等级"`
	PendingLevel     int32          `comment:"将要升到的等级"`
	PendingLevelTime int64          `comment:"将要升到的等级截止时间"`
	HpLevel          int32          `comment:"天赋生命值等级"`
	AtkLevel         int32          `comment:"天赋攻击力等级"`
	GoldLevel        int32          `comment:"天赋初始金币等级"`
	Skills           []int32        `comment:"技能id"`
	UnlockSoilder    int32          `comment:"解锁带入局内的士兵"`
	HasDone          map[int64]bool `comment:"进化是否提交材料"`
}

// @genEnum:`value:"true"`
type LordType struct {
	HP   int32 `enum:"1" comment:"生命值等级"`
	Atk  int32 `enum:"2" comment:"攻击力等级"`
	Gold int32 `enum:"3" comment:"初始金币等级"`
}
