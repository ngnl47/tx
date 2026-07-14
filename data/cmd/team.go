package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"SyncResp"
type Team_SetTeam struct {
	TeamType data.TeamType
	Heros    map[int32]int32 `comment:"英雄列表 key是位置(0开始), value是英雄id"`
}
