package cmd

// @genReq:""
// @comment:"玩家心跳"
type System_Ping struct{}

// @genReq:""
// @comment:"玩家连接断开"
type System_Disconnect struct{}

// @genResp:""
// @comment:"踢掉线"
type KickRole struct {
	Reason        string `comment:"原因"`
	IsCloseClient bool   `comment:"是否强制关闭客户端"`
}

// @genResp:""
// @comment:"全服通知"
type GlobalNotify struct {
	Content string `comment:"通知内容"`
	Num     int32  `comment:"通知次数"`
}
