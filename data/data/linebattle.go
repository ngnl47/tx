package data

// @genData:`value:"true"`
// @comment:`value:""`
type LB_FieldInfo struct {
	Players          []*LB_PlayerInfo
	Citys            []*LB_CityInfo
	Streets          []*LB_StreetInfo
	Soldiers         []*LB_SoldierInfo
	IsAutoAttackMod  bool
	Gold             int32
	BattleEndTime    int64
	GiveUpNeedTime   int64
	GranaryTimeDelta int64   `comment:"粮仓产生时间间隔"`
	GranaryCntDelta  int64   `comment:"粮仓产生数量"`
	SkillTimer       float32 `comment:"落雷计时器" ignoreClient:"true"`
	IsLater          bool    `comment:"是否是迟到者" `
}

// @genData:`value:"true"`
// @comment:`value:"LB_StreetInfo"`
type LB_StreetInfo struct {
	Id    string  `comment:"StreetId"`
	State bool    `comment:"State"`
	Timer float32 `comment:"SendSoldierTimer"`
}

// @genData:`value:"true"`
// @comment:`value:""`
type LB_CityInfo struct {
	CityId           int32
	Camp             int32
	TotalHp          float32
	AddLifeTimer     float32
	AddGoldTimer     float32
	Star             int32
	SoldierCount     int32
	SpawnSoliderCD   float32
	SpawnGoldCD      float32
	GranaryGoldTimer float32
	HasBuff          bool
	OccupyItemId     int32   `comment:"占领奖励"`
	SkillTimer       float32 `comment:"落雷计时器"`
}

// @genData:`value:"true"`
// @comment:`value:"LB_SoldierInfo"`
type LB_SoldierInfo struct {
	Id     string   `comment:"士兵SoldierId"`
	FcId   int32    `comment:"FromCityId"`
	FcHp   float32  `comment:"FromCityHp"`
	FcSCnt int32    `comment:"FromCitySoldierCount"`
	TcId   int32    `comment:"ToCityId"`
	Sid    string   `comment:"StreetId"`
	Cnt    int32    `comment:"SendCnt"`
	Camp   int32    `comment:"Camp"`
	P      float32  `comment:"Progress"`
	Speed  float32  `comment:"Speed"`
	Pass   bool     `comment:"JustPassBy"`
	DSoIds []string `comment:"DestroySoldierId"`
	Hp     float32  `comment:"TotalHp"`
}

// @genEnum:`value:"true"`
// @comment:`value:""`
type EMLB_SoldierSpType struct {
	None     int32 `enum:"0"`
	DragonTu int32 `enum:"1"`
}

// @genData:`value:"true"`
// @comment:`value:""`
type LB_PlayerInfo struct {
	Role                         *GDRoomMember
	CurrentCityId                int32
	KillCount                    int32
	KillCountReachedList         []int
	OwnCitys                     []int32
	IsGiveUp                     bool
	IsDie                        bool `comment:"是否被击败"`
	IsWin                        bool `comment:"是否最终胜利"`
	DieIndex                     int
	UsedFreeQuickSendSoldierFlag bool
	KingCityFirstAddSoldierFlag  bool
	IsAuto                       bool
	Gold                         int32
	RecruitFromWeakTimer         float64
	FirstFreeRecruit             bool
	SoldierUIType                int32              `comment:"士兵UI类型" ignoreClient:"true"`
	AIType                       int32              `comment:"AI类型" ignoreClient:"true"`
	SoldierId                    int32              `comment:"士兵编号"`
	SpType                       EMLB_SoldierSpType `comment:"SP类型"`
}
