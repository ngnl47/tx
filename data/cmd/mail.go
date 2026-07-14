package cmd

import (
	"evo-data/data"
	data2 "git.hortorgames.com/orange/mandarin/datadef/v3/omail/data"
)

// @genReq:""
// @linkResp:"Mail_GetListResp"
type Mail_GetList struct {
	LastId   int64   `comment:"最后的编号"`
	Size     int     `comment:"获取数量"`
	Category []int64 `comment:"分类"`
}

// @genReq:""
// @linkResp:"Mail_GetListResp"
type Mail_GetBattlefieldReportList struct {
	LastId   int64 `comment:"最后的编号"`
	Category int64 `comment:"分类"`
	Size     int   `comment:"获取数量"`
}

// @genReq:""
// @linkResp:"Mail_GetListResp"
type Mail_GetLegionList struct {
	LastId int64 `comment:"最后的编号"`
	Size   int   `comment:"获取数量"`
}

// @genResp:""
type Mail_GetListResp struct {
	List []*data2.OMailUserMail `comment:"邮件列表"`
	Left int                    `comment:"剩余数量"`
}

// @genReq:""
// @linkResp:"Mail_ChangeStateResp"
type Mail_ChangeState struct {
	MailId int64            `comment:"邮件编号"`
	State  data2.OMailState `comment:"状态"`
}

// @genResp:""
type Mail_ChangeStateResp struct {
	Mail *data2.OMailUserMail `comment:"变更后的邮件"`
}

// @genReq:""
// @linkResp:"Mail_ClaimAttachmentResp"
type Mail_ClaimAttachment struct {
	MailId int64 `comment:"邮件编号"`
}

// @genResp:""
type Mail_ClaimAttachmentResp struct {
	Role   *data.GDRole         `comment:"角色状态"`
	Reward []*data.GDRewardData `comment:"附件奖励内容"`
	Mail   *data2.OMailUserMail `comment:"变更后的邮件"`
}

// @genReq:""
// @linkResp:"Mail_ClaimAllAttachmentResp"
type Mail_ClaimAllAttachment struct {
	Category int64 `comment:"分类"`
}

// @genResp:""
type Mail_ClaimAllAttachmentResp struct {
	Role   *data.GDRole           `comment:"角色状态"`
	Reward []*data.GDRewardData   `comment:"附件奖励内容"`
	List   []*data2.OMailUserMail `comment:"邮件列表"`
}

// @genReq:""
// @linkResp:"Mail_DeleteAllUnreceivedResp"
type Mail_DeleteAllUnreceived struct {
}

// @genReq:""
// @linkResp:"Mail_DeleteAllUnreceivedResp"
type Mail_DeleteAllUnreceivedWithCategory struct {
	Category int64 `comment:"分类"`
}

// @genResp:""
type Mail_DeleteAllUnreceivedResp struct {
	List []*data2.OMailUserMail `comment:"邮件列表"`
	Left int                    `comment:"剩余数量"`
}

// @genResp:""
type Mail_RetractResp struct {
	RoleId int64 `comment:"发送者ID"`
}

// @genReq:""
// @linkResp:"Mail_GetMtlShortInfoResp"
// @comment:"请求邮件动态多语言文本简讯"
type Mail_GetMtlShortInfo struct {
}

// @genResp:""
type Mail_GetMtlShortInfoResp struct {
	List []*data.GDShortMtl `comment:"多语言简讯列表"`
}

// @genReq:""
// @linkResp:"Mail_GetMtlInfoResp"
// @comment:"请求邮件动态多语言文本"
type Mail_GetMtlInfo struct {
	List []string `comment:"多语言 id 列表"`
	Lang string   `comment:"语言"`
}

// @genResp:""
type Mail_GetMtlInfoResp struct {
	MtlDataMap map[string]string `comment:"多语言列表"`
	Lang       string            `comment:"语言"`
}
