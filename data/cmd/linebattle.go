package cmd

import (
	commonData "evo-data/data"
	"evo-data/room/data"
)

// @genResp:""
// @comment:"LB_SoldierArrivedNotify"
type LB_SAN struct {
	SoId            string  `comment:"SoldierId"`
	CId             int32   `comment:"CityId"`
	AC              int32   `comment:"AttackCamp"`
	DC              int32   `comment:"DefCamp"`
	CHP             float32 `comment:"CityHp"`
	HasScore        bool    `comment:"HasScore"` //只有攻击非中立城池才会计算杀敌数   服务器也要处理攻击非中立城池的情况（camp>0）
	CKCnt           int32   `comment:"CityKillCnt"`
	SoKCnt          int32   `comment:"SoldierKillCnt"`
	CityCampChanged bool    `comment:"CityCampChanged"`
	SoCnt           int32   `comment:"SoldierCnt"`
}

// @genReq:""
// @linkResp:"LB_UseStreetResp"
// @comment:""
type LB_UseStreet struct {
	RoomId   string
	StreetId string
	State    bool
}

// @genEnum:""
// @comment:"战斗类型"
type EMUseStreetType struct {
	Player   int32 `enum:"0" comment:"玩家主动操作"`
	Auto     int32 `enum:"1" comment:"自动"`
	Assemble int32 `enum:"2" comment:"集结"`
	AI       int32 `enum:"3" comment:"AI操作"`
}

// @genData:""
// @comment:"LB_BatchStreetInfo"
type LB_BatchStreetInfo struct {
	Type    EMUseStreetType             `comment:"Type"`       // 0玩家主动操作，1自动,2集结(用枚举)
	Citys   []int32                     `comment:"ClearCitys"` //集结、进攻、自动攻击,扣除城市被打下来
	Streets []*commonData.LB_StreetInfo `comment:"StreetInfos"`
}

// @genResp:""
// @comment:"LB_UseStreetNotify"
type LB_USN struct {
	List []*LB_BatchStreetInfo `comment:"StreetBatchInfos"`
}

// @genResp:""
// @comment:"主城升星通知"
type LB_CenterCityStarUpNotify struct {
	CityId         int32   `comment:"城池Id"`
	Star           int32   `comment:"星级"`
	AddLifeTimer   float32 `comment:"创建士兵的计时器"`
	SpawnSoliderCD float32 `comment:"创建士兵的CD"`
}

// @genResp:""
// @comment:"产兵速度通知"
type LB_CityInfoChangeNotify struct {
	Citys []*commonData.LB_CityInfo `comment:"城池信息"`
}

// @genResp:""
// @comment:""
type LB_StreetAttachEachNotify struct {
	ASoldierId  string
	BSoldierId  string
	AStreetId   string
	BStreetId   string
	ARecvDamage float64
	BRecvDamage float64
	AKillCnt    int32
	BKillCnt    int32
	ATotalHp    float32
	BTotalHp    float32
	ADie        bool
	BDie        bool
}

// @genResp:""
// @comment:"LB_CreateSoldierNotify"
type LB_SCN struct {
	Soldiers []*commonData.LB_SoldierInfo
	/*SoldierId  string
	FromCityId int32
	ToCityId   int32
	StreetId   string
	SendCnt    int32
	Camp       int32
	JustPassBy bool*/
}

// @genReq:""
// @linkResp:"LB_BuySoldierResp"
// @comment:""
type LB_BuySoldier struct {
	RoomId string
}

// @genEnum:""
type CityAddSoldierType struct {
	Steal              int32 `enum:"0"`
	Buy                int32 `enum:"1"`
	KingCityCaptureAdd int32 `enum:"2"`
	KillAdd            int32 `enum:"3"`
	DragonShui         int32 `enum:"4" comment:"水龙城加兵"`
	DragonHuo          int32 `enum:"5" comment:"火龙城减兵"`
	LightningSub       int32 `enum:"6" comment:"闪电减兵"`
}

// @genData:""
type CityAddSoldierInfo struct {
	Type       CityAddSoldierType
	CampId     int32
	CityId     int32
	CityHp     float32
	AddCnt     int32
	SoldierCnt int32
}

// @genResp:""
// @comment:""
type LB_AddSoldierNotify struct {
	CityInfos []*CityAddSoldierInfo
}

// @genReq:""
// @linkResp:"LB_QuickSendSoldierResp"
// @comment:""
type LB_QuickSendSoldier struct {
	RoomId string
}

// @genReq:""
// @linkResp:"LB_AssembleCityResp"
// @comment:""
type LB_AssembleCity struct {
	RoomId string
	CityId int32
}

// @genReq:""
// @comment:""
// @linkResp:"LB_SetAutoAttackResp"
type LB_SetAutoAttack struct {
	RoomId string
}

// @genResp:""
// @comment:""
type LB_DestroySoldierNotify struct {
	SoldierId string
}

// @genReq:""
// @linkResp:"LB_GiveUpResp"
// @comment:""
type LB_GiveUp struct {
	RoomId string
}

// @genReq:""
// @linkResp:"LB_GetFieldInfoResp"
type LB_GetFieldInfo struct {
	RoomId string `comment:"房间id"`
}

// @genResp:""
// @comment:""
type LB_GetFieldInfoNotify struct {
	FieldInfo commonData.LB_FieldInfo
}

// @genResp:""
type LB_BattleStartNotify struct {
	RoomId    string
	FieldInfo commonData.LB_FieldInfo
}

// @genResp:""
// @comment:""
type LB_GiveUpNotify struct {
	CampId     int32
	RoleId     int64
	ClearCitys []int32
}

// @genResp:""
// @comment:""
type LB_BattleResultNotify struct {
	CampId     int32
	Rewards    []*commonData.GDRewardData
	Score      int32
	SelfRank   int32
	Roles      []*data.NotifyEvalRole
	FinishType EMLBFinishType
}

// @genEnum:""
// @comment:"战斗类型"
type EMLBFinishType struct {
	Win    int32 `enum:"0"`
	Lose   int32 `enum:"1"`
	GiveUp int32 `enum:"2"`
}

// @genReq:""
type LB_Prepare struct {
	RoomId string `comment:"房间id"`
}

// @genResp:""
// @comment:"玩家准备好通知"
type LB_PrepareResp struct {
	Cur int32 `comment:"当前准备人数"`
	Max int32 `comment:"最大人数"`
}

// @genReq:""
type LB_SayHello struct {
	RoomId string `comment:"房间id"`
}

// @genResp:""
// @comment:""
type LB_SayHelloResp struct {
}

// @genReq:""
type LB_ExcuteGmCmd struct {
	RoomId  string `comment:"房间id"`
	Commond string
}

// @genEnum:""
// @comment:""
type EMLB_EventType struct {
	Gold          int32 `enum:"0"`
	AutoAttack    int32 `enum:"1"`
	SoldierChange int32 `enum:"2"`
}

// @genData:""
// @comment:""
type LB_EventInfo struct {
	CampId       int32
	Type         EMLB_EventType
	NumberParams []float64
	StringParams []string
	BoolParam    bool
}

// @genResp:""
// @comment:""
type LB_EventNotify struct {
	Infos []*LB_EventInfo
}

// @genData:""
// @comment:""
type LB_SkillInfo struct {
	CampId    int32
	SkillId   int32
	Type      EMLB_EventType
	DataParam any
}

// @genResp:""
// @comment:""
type LB_SkillNotify struct {
	Infos []*LB_SkillInfo
}

// @genResp:""
// @comment:""
type LB_CityCampChangeNotify struct {
	CityId         int32
	NewCamp        int32
	OldCamp        int32
	CityHp         float64
	IsAI           bool
	AddLifeTimer   float64
	AddGoldTimer   float64
	SpawnSoliderCD float64
	SpawnGoldCD    float64
	SoldierCnt     int32
	HasBuff        bool
	SkillTimer     float32 `comment:"落雷计时器"`
}

// @genResp:""
// @comment:""
type LB_AllPlayerReadyNotify struct {
	FieldInfo commonData.LB_FieldInfo
}

// @genResp:""
// @comment:"失败阵营在路线上的士兵"
type LB_FailSoldierOnStreetNotify struct {
	SoldierIds []string `comment:"在路线中的士兵id数组"`
	FailCampId int32    `comment:"失败阵营id"`
}
