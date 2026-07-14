package data

// @genEnum:`value:"true"`
type MatchState struct {
	Default  int32 `enum:"0" comment:"默认"`
	Matching int32 `enum:"1" comment:"匹配中"`
	Matched  int32 `enum:"2" comment:"已匹配"`
	InBattle int32 `enum:"3" comment:"战斗中"`
	Ended    int32 `enum:"4" comment:"结束"`
}

// @genData:`value:"true"`
type GDMatchInfo struct {
	MatchState  MatchState
	StartTime   int64  `comment:"匹配开始时间"`
	SuccessTime int64  `comment:"匹配成功时间"`
	RoomId      string `comment:"房间id"`
	RoomRpcId   string `comment:"房间rpc id"`
	MapId       int32  `comment:"地图 id" ignore:"true"`
	RoleList    []*GDRoomMember
}
