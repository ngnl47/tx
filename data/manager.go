package data

import "time"

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"adminId" idField:"adminId" collection:"admins"`
// @customExport:`value:"true"`
// @comment:`value:"管理员"`
type GDAdmin struct {
	AdminId      int32     `comment:"管理员id" mongodb:"index, unique"`
	Email        string    `comment:"邮箱" mongodb:"index"`
	Account      string    `comment:"登陆帐号" mongodb:"index"`
	Phone        string    `comment:"手机号码" mongodb:"index"`
	Password     string    `comment:"密码"`
	Name         string    `comment:"名字"`
	Avatar       string    `comment:"头像"`
	Status       int32     `comment:"状态"`
	Remark       string    `comment:"备注"`
	AdminRoleIds []int32   `comment:"管理角色" mongodb:"index"`
	UpdateTime   time.Time `comment:"修改时间"`
	CreateTime   time.Time `comment:"创建时间"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"adminId" idField:"adminId" collection:"adminsV2"`
// @customExport:`value:"true"`
// @comment:`value:"管理员 V2"`
type Admin GDAdmin

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"roleId" idField:"roleId" collection:"adminRoles"`
// @customExport:`value:"true"`
// @comment:`value:"管理角色"`
type GDAdminRole struct {
	RoleId        int32          `comment:"管理角色id" mongodb:"index, unique"`
	Name          string         `comment:"名字"`
	Description   string         `comment:"头像"`
	Enabled       bool           `comment:"启用"`
	Sort          int32          `comment:"备注"`
	AdminIds      []int32        `comment:"管理员列表"`
	PermissionIds map[int32]bool `comment:"权限列表"`
	UpdateTime    time.Time      `comment:"修改时间"`
	CreateTime    time.Time      `comment:"创建时间"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"roleId" idField:"roleId" collection:"adminRolesV2"`
// @customExport:`value:"true"`
// @comment:`value:"管理角色 V2"`
type AdminRole GDAdminRole

// @genEnum:`value:"true"`
// @customExport:`value:"true"`
type EMPermissionType struct {
	Group int32 `enum:"1" comment:"分组"`
	Menu  int32 `enum:"2" comment:"菜单"`
	Api   int32 `enum:"3" comment:"接口"`
	Dot   int32 `enum:"4" comment:"权限点"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"pId" idField:"pId" collection:"permissions"`
// @customExport:`value:"true"`
// @comment:`value:"管理权限"`
type GDPermission struct {
	PId         int32            `comment:"权限id" mongodb:"index, unique"`
	ParentId    int32            `comment:"父级借点"`
	Name        string           `comment:"权限名称"`
	Code        string           `comment:"权限编码"`
	Type        EMPermissionType `comment:"权限类型"`
	Path        string           `comment:"菜单访问地址"`
	ViewPath    string           `comment:"视图路径"`
	ApiPath     string           `comment:"接口地址"`
	Icon        string           `comment:"图标"`
	Hidden      bool             `comment:"隐藏"`
	Enabled     bool             `comment:"启用"`
	Opened      bool             `comment:"打开组"`
	Sort        int32            `comment:"排序"`
	Description string           `comment:"描述"`
	UpdateTime  time.Time        `comment:"修改时间"`
	CreateTime  time.Time        `comment:"创建时间"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"pId" idField:"pId" collection:"permissionsV2"`
// @customExport:`value:"true"`
// @comment:`value:"管理权限 V2"`
type Permission GDPermission

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"id" idField:"id" collection:"operationLog"`
// @customExport:`value:"true"`
// @comment:`value:"操作log"`
type GDOperationLog struct {
	Id                  int64     `comment:"Log编号" mongodb:"index"`
	AdminId             int32     `comment:"管理员编号" mongodb:"index"`
	AdminName           string    `comment:"管理员姓名"`
	ApiLabel            string    `comment:"接口名称" mongodb:"index"`
	ApiPath             string    `comment:"接口地址" mongodb:"index:api_create"`
	ApiMethod           string    `comment:"接口提交方法"`
	Params              string    `comment:"操作参数"`
	IP                  string    `comment:"IP"`
	Browser             string    `comment:"浏览器"`
	Os                  string    `comment:"操作系统"`
	Device              string    `comment:"设备"`
	BrowserInfo         string    `comment:"浏览器信息"`
	ElapsedMilliseconds int64     `comment:"耗时（毫秒）"`
	Status              bool      `comment:"操作状态"`
	Msg                 string    `comment:"操作消息"`
	Result              string    `comment:"操作结果"`
	CreateTime          time.Time `comment:"创建时间" mongodb:"index:api_create,index"`
}

// @genEnum:`value:"true"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
type EMLegionOperationType struct {
	ResetLogo     int32 `enum:"1" comment:"重置头像"`
	ResetName     int32 `enum:"2" comment:"重置名称"`
	ClearAnnounce int32 `enum:"3" comment:"清空公告"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"legionId" idField:"legionId" collection:"legionOperationLog"`
// @customExport:`value:"true"`
// @comment:`value:"俱乐部操作日志"`
type GDLegionOperationLog struct {
	LegionId int64                     `comment:"俱乐部编号" mongodb:"index, unique"`
	Logs     []*LegionOperationLogItem `comment:"日志明细"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @comment:`value:"俱乐部操作日志明细"`
type LegionOperationLogItem struct {
	OpName        string                `comment:"操作人"`
	OperationType EMLegionOperationType `comment:"操作类型"`
	Data          string                `comment:"数据"`
	CreateTime    int64                 `comment:"操作时间"`
}

// @genEnum:`value:"true"`
// @ignoreClient:`value:"true"`
// @customExport:`value:"true"`
type EMRoleOperationType struct {
	ResetName             int32 `enum:"1" comment:"重置昵称"`
	ForbidMsg             int32 `enum:"2" comment:"禁言"`
	UnForbidMsg           int32 `enum:"3" comment:"解除禁言"`
	RecruitChatMsg        int32 `enum:"4" comment:"撤回消息"`
	RecruitMail           int32 `enum:"5" comment:"撤回邮件"`
	Ban                   int32 `enum:"6" comment:"封禁"`
	UnBan                 int32 `enum:"7" comment:"解封"`
	ResetAvatar           int32 `enum:"8" comment:"重置头像"`
	NMRankDel             int32 `enum:"9" comment:"十殿排行榜删除"`
	NMForbid              int32 `enum:"10" comment:"十殿禁赛"`
	ResetHeroSN           int32 `enum:"11" comment:"重置英雄皮肤名字"`
	SpecialList           int32 `enum:"12" comment:"特殊名单"`
	ResetTeamName         int32 `enum:"13" comment:"重置阵容名称"`
	ClearRoleLegionApply  int32 `enum:"14" comment:"清空俱乐部申请留言"`
	ForbidRoleApplyReason int32 `enum:"15" comment:"禁止申请俱乐部时留言"`
	ForbidAction          int32 `enum:"16" comment:"禁止操作"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @storage:`mode:"mongo"`
// @mongodb:`routeType:"manager" routeBy:"roleId" idField:"roleId" collection:"roleOperationLog"`
// @customExport:`value:"true"`
// @comment:`value:"玩家操作日志"`
type GDRoleOperationLog struct {
	RoleId int64                   `comment:"玩家编号" mongodb:"index, unique"`
	Logs   []*RoleOperationLogItem `comment:"日志明细"`
}

// @genData:`value:"true"`
// @ignoreClient:`value:"true"`
// @comment:`value:"玩家操作日志明细"`
type RoleOperationLogItem struct {
	OpName        string              `comment:"操作人"`
	OperationType EMRoleOperationType `comment:"操作类型"`
	Data          string              `comment:"数据"`
	Data1         string              `comment:"数据1"`
	CreateTime    int64               `comment:"操作时间"`
}
