package cmd

import "evo-data/data"

// @genResp:""
type SyncResp struct {
	Role *data.GDRole
}

// @genResp:""
type SyncRewardResp struct {
	Role   *data.GDRole         `comment:"角色状态"`
	Reward []*data.GDRewardData `comment:"挂机奖励"`
}

// @genReq:""
// @linkResp:"Role_GetRoleInfoResp"
type Role_GetRoleInfo struct {
	Platform      string `comment:"平台,debug,hortor"`
	PlatformExt   string `comment:"平台,debug,hortor"`
	InviteUid     int64  `comment:"邀请玩家id"`
	ClientVersion string `comment:"客户端版本号"`
	Scene         string `comment:"微信小程序场景"`
}

// @genResp:""
// @comment:"获取角色信息返回"
type Role_GetRoleInfoResp struct {
	SyncResp
	IsWhiteRole    bool                  `comment:"白名单"`
	CreateRoleType data.EMCreateRoleType `comment:"支付SDK创建账号类型"`
}

// @genReq:""
// @linkResp:"Role_ExeGmCmdResp"
type Role_ExeGmCmd struct {
	Cmd string
}

// @genResp:""
// @comment:"执行gm指令返回"
type Role_ExeGmCmdResp struct {
	SyncResp
}
