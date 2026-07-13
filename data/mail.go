package data

// @genEnum:`value:"true"`
// @comment:`value:"邮件分类"`
type EMMailCategory struct {
	CategorySystemMail       int32 `enum:"0" comment:"系统邮件"`
	CategoryDailyMail        int32 `enum:"1" comment:"日常邮件"`
	CategoryPublicNoticeMail int32 `enum:"2" comment:"游戏公告"`
	CategoryLegionMail       int32 `enum:"3" comment:"军团邮件"`
}

// @genData:`value:"true"`
// @customExport:`value:"true"`
// @comment:`value:"发送邮件限制相关信息"`
type GDSendMail struct {
	LastTime int64   `comment:"最后发送时间"`
	RoleList []int64 `comment:"发送角色列表"`
}
