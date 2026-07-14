package cmd

// @genReq:""
// @linkResp:"Boss_GetStateResp"
// @comment:"获取BOSS最大伤害"
type Boss_GetState struct {
	BossId int32 `comment:"BOSS编号"`
}

// @genResp:""
type Boss_GetStateResp struct {
	Hurt int64 `comment:"最大伤害值"`
}
