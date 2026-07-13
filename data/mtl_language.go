package data

import "time"

// @genData:`value:"true"`
// @storage:`mode:"redis"`
// @redis:`routeType:"main" routeBy:"id" idField:"id" key:"mtl"`
// @comment:`value:"邮件多语言动态文本"`
type GDMtl struct {
	Id           string                   `comment:"文本id"`
	Desc         string                   `comment:"文本描述"`
	Type         int32                    `comment:"类型划分（0邮件保留60天 ｜ 1活动，活动的时间是永久）"`
	LanguageMap  map[string]*LanguageInfo `comment:"多语言文本字典"`
	UpdateTime   time.Time                `comment:"有效时间"`
	CreatedTime  time.Time                `comment:"修改时间"`
	LanguageList []*LanguageInfo          `comment:"多语言文本字典（gm 用的，客户端忽略）"`
}

// @genData:`value:"true"`
type LanguageInfo struct {
	Language string `comment:"语言"`
	Content  string `comment:"文本内容"`
}

// @genData:`value:"true"`
// @comment:`value:"邮件多语言标签"`
type MailContent struct {
	Id       string     `comment:"对应邮件多语言表 id"`
	Params   []string   `comment:"动态参数列表(兼容老版本，以后用 ParamMtl)"`
	ParamMtl []ParamMtl `comment:"动态参数列表"`
}

// @genData:`value:"true"`
// @comment:`value:"参数多语言标签"`
type ParamMtl struct {
	Param string       `comment:"参数"`
	Type  ParamMtlType `comment:"多语言解析类型 （0 不需要解析，用原始文本 ｜ 1：去 language 表读取）"`
}

// @genEnum:`value:"true"`
// @comment:`value:"邮件参数多语言解析类型"`
type ParamMtlType struct {
	None          int32 `enum:"0" comment:"不需要解析，用原始文本"`
	LanguageTable int32 `enum:"1" comment:"去 language 表读取"`
}

// @genData:`value:"true"`
// @comment:`value:"多语言标签简讯"`
type GDShortMtl struct {
	Id         string    `comment:"文本id"`
	UpdateTime time.Time `comment:"修改时间"`
}
