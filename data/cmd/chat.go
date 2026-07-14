package cmd

import (
	"evo-data/data"
)

// @genReq:""
// @linkResp:"System_SendChatMessageResp"
// @comment:"发送聊天消息"
type System_SendChatMessage struct {
	Channel data.EMChatMsgChannelType `comment:"消息频道"`
	MsgType data.EMChatMsgType        `comment:"消息类型"`
	Msg     string                    `comment:"消息文本"`
	EmojiId int32                     `comment:"表情编号"`
	Extra   *data.GDChatMsgExt        `comment:"额外信息"`
}

// @genResp:""
// @comment:"发送聊天消息返回"
type System_SendChatMessageResp struct {
	Role    *data.GDRole        `comment:"角色状态"`
	Legion  *data.GDLegion      `comment:"军团状态"`
	Message *data.GDChatMessage `comment:"新消息"`
}

// @genReq:""
// @linkResp:"System_GetChatMessageResp"
// @comment:"获取聊天消息"
type System_GetChatMessage struct {
	Channel []data.EMChatMsgChannelType `comment:"消息频道"`
}

// @genResp:""
// @comment:"获取聊天消息返回"
type System_GetChatMessageResp struct {
	MessageMap map[int32][]*data.GDChatMessage `comment:"聊天列表"`
}

// @genResp:""
type System_NewChatMessageNotify struct {
	ChatMessage *data.GDChatMessage `comment:"聊天新消息"`
}

// @genReq:""
// @linkResp:"System_SendMailResp"
type System_SendMail struct {
	RoleId  int64  `comment:"接收者角色编号"`
	Content string `comment:"邮件内容"`
}

// @genResp:""
type System_SendMailResp struct{}
