package data

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"武将"`
type GDHero struct {
	HeroId         int32                    `comment:"武将编号"`
	Level          int32                    `comment:"武将等级"`
	Order          int32                    `comment:"武将品阶"`
	Star           int32                    `comment:"武将星级"`
	Color          int32                    `comment:"武将品质"`
	SkinId         int32                    `comment:"穿戴皮肤"`
	Power          int64                    `comment:"战力"`
	Attack         int64                    `comment:"攻击力"`
	Defense        int64                    `comment:"防御力"`
	Hp             int64                    `comment:"血量"`
	Speed          int64                    `comment:"攻击速度"`
	AttackExt      float32                  `comment:"攻击力加成"`
	DefenseExt     float32                  `comment:"防御力加成"`
	HpExt          float32                  `comment:"血量加成"`
	Attribute      map[int32]float64        `comment:"其它属性"`
	BattleTeamSlot int32                    `comment:"所在阵容槽位"`
	AttackSkill    GDSkill                  `comment:"普攻技能"`
	ActiveSkill    GDSkill                  `comment:"主动技能"`
	PassiveSkill1  GDSkill                  `comment:"被动技能1"`
	PassiveSkill2  GDSkill                  `comment:"被动技能2"`
	Equipment      map[EMEquipPart]*GDEquip `comment:"英雄装备"`
	EquipmentSetId int32                    `comment:"装备套装编号"`
	SpEquipment    map[int32]*GDSpEquip     `comment:"专属装备"`

	BookPointList []int32 `comment:"待领取图鉴积分列表"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"技能"`
type GDSkill struct {
	SkillId    int32 `comment:"技能编号"`
	Level      int32 `comment:"技能等级"`
	Active     bool  `comment:"是否激活"`
	Affiliated int32 `comment:"所属编号"`
}

// @genEnum:`value:"true"`
// @ignoreClient:`value:"true"`
// @comment:`value:"武将属性加成原因"`
type EMHeroAttrReason struct {
	HeroBase            int32 `enum:"1" comment:"武将基础"`
	HeroLevel           int32 `enum:"2" comment:"武将升级"`
	HeroStar            int32 `enum:"3" comment:"武将升星"`
	LordLevel           int32 `enum:"4" comment:"主公等级"`
	EquipFixed          int32 `enum:"5" comment:"装备固定"`
	EquipSet            int32 `enum:"6" comment:"装备套装"`
	HeroSkill           int32 `enum:"7" comment:"武将技能"`
	LordTech            int32 `enum:"8" comment:"主公科技"`
	SpEquipFixed        int32 `enum:"9" comment:"专属装备固定"`
	SpEquipPer          int32 `enum:"10" comment:"专属装备百分比加成"`
	DressPer            int32 `enum:"11" comment:"装扮百分比加成"`
	HolyBeastFixed      int32 `enum:"12" comment:"神机兽固定值"`
	HolyBeastEquipPer   int32 `enum:"13" comment:"神机装备百分比加成"`
	HolyBeastEquipFixed int32 `enum:"14" comment:"神机装备固定值"`
}
