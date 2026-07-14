package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Auth_LoginResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"GM验证登陆"
type Auth_Login struct {
	Username string `comment:"用户名"`
	Password string `comment:"密码"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Auth_LoginResp struct {
	AdminInfo *data.GDAdminInfo `comment:"管理员信息"`
	Token     string            `comment:"管理员令牌"`
}

// @genReq:""
// @linkResp:"Account_AdminInfoResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"获取管理信息"
type Account_AdminInfo struct{}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Account_AdminInfoResp struct {
	AdminInfo *data.GDAdminInfo `comment:"管理员信息"`
}

// @genReq:""
// @linkResp:"Account_GetConfigResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"获取服务器配置"
type Account_GetConfig struct {
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Account_GetConfigResp struct {
	ConfigList map[string]any
}

// @genReq:""
// @linkResp:"Account_UpdateInfoResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"更新帐号信息"
type Account_UpdateInfo struct {
	Email  string `comment:"邮箱" mongodb:"index"`
	Phone  string `comment:"手机号码" mongodb:"index"`
	Name   string `comment:"名字"`
	Avatar string `comment:"头像"`
	Status int32  `comment:"状态"`
	Remark string `comment:"备注"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
// @comment:"更新帐号信息返回"
type Account_UpdateInfoResp struct {
	AdminInfo *data.GDAdmin `comment:"管理员信息"`
}

// @genReq:""
// @linkResp:"Account_UpdatePwdResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"更新帐号密码"
type Account_UpdatePwd struct {
	OldPassword     string `comment:"旧密码"`
	Password        string `comment:"新密码"`
	ConfirmPassword string `comment:"确认新密码"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
// @comment:"更新帐号密码返回"
type Account_UpdatePwdResp struct{}

// @genReq:""
// @linkResp:"Account_MyLogResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"自己的日志"
type Account_MyLog struct {
	PageSize int64 `comment:"每页条数"`
	Page     int64 `comment:"当前页数"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
// @comment:"自己的日志返回"
type Account_MyLogResp struct {
	List      []*data.GDOperationLog `comment:"日志列表"`
	Page      int64                  `comment:"当前页码"`
	PageCount int64                  `comment:"总页数"`
	PageSize  int64                  `comment:"每页大小"`
	Total     int64                  `comment:"总数量"`
}

// @genReq:""
// @linkResp:"AdminRole_ListResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色管理列表"
type AdminRole_List struct {
	PageSize int64       `comment:"每页条数"`
	Page     int64       `comment:"当前页数"`
	Filter   interface{} `comment:"查询条件"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type AdminRole_ListResp struct {
	List      []*data.AdminRole `comment:"角色列表"`
	Page      int64             `comment:"当前页码"`
	PageCount int64             `comment:"总页数"`
	PageSize  int64             `comment:"每页大小"`
	Total     int64             `comment:"总数量"`
}

// @genReq:""
// @linkResp:"AdminRole_CreateResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色创建"
type AdminRole_Create struct {
	Name        string  `comment:"角色名"`
	Enabled     int32   `comment:"角色是否启用"`
	Permission  []int32 `comment:"角色权限"`
	Description string  `comment:"角色说明"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type AdminRole_CreateResp struct {
	RoleId int32 `comment:"角色ID"`
}

// @genReq:""
// @linkResp:"AdminRole_DeleteResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色删除"
type AdminRole_Delete struct {
	RoleId []int32 `comment:"角色ID"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type AdminRole_DeleteResp struct{}

// @genReq:""
// @linkResp:"AdminRole_UpdateResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色更新"
type AdminRole_Update struct {
	RoleId      int32   `comment:"角色ID"`
	Name        string  `comment:"角色名"`
	Permission  []int32 `comment:"角色权限"`
	Description string  `comment:"角色说明"`
	Enabled     int32   `comment:"角色是否启用"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type AdminRole_UpdateResp struct {
	RoleId int32 `comment:"角色ID"`
}

// @genReq:""
// @linkResp:"Permission_GetListResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"获取权限"
type Permission_GetList struct {
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_GetListResp struct {
	List []*data.Permission `comment:"权限列表"`
}

// @genReq:""
// @linkResp:"Permission_CreateResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"创建新权限"
type Permission_Create struct {
	ParentId    int32                 `comment:"父级节点"`
	Name        string                `comment:"权限名称"`
	Code        string                `comment:"权限编码"`
	Type        data.EMPermissionType `comment:"权限类型"`
	Path        string                `comment:"菜单访问地址"`
	ViewPath    string                `comment:"视图路径"`
	ApiPath     string                `comment:"接口地址"`
	Icon        string                `comment:"图标"`
	Hidden      bool                  `comment:"隐藏"`
	Opened      bool                  `comment:"打开组"`
	Sort        int32                 `comment:"排序"`
	Description string                `comment:"描述"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_CreateResp struct {
	PId int32 `comment:"权限ID"`
}

// @genReq:""
// @linkResp:"Permission_ImportResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"导入权限"
type Permission_Import struct{}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_ImportResp struct{}

// @genReq:""
// @linkResp:"Permission_ExportResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"导出权限"
type Permission_Export struct {
	IncludingData bool `comment:"包含数据"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_ExportResp struct {
	Data string `comment:"文件"`
}

// @genReq:""
// @linkResp:"Permission_SyncResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"导入Test权限到本地"
type Permission_Sync struct {
	IncludingData bool `comment:"包含数据"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_SyncResp struct {
	Data string `comment:"文件"`
}

// @genReq:""
// @linkResp:"Admin_DeleteResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"删除权限"
type Permission_Delete struct {
	PIds []int32 `comment:"权限ID"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_DeleteResp struct{}

// @genReq:""
// @linkResp:"Permission_UpdateResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"更新权限"
type Permission_Update struct {
	PId         int32                 `comment:"权限ID"`
	ParentId    int32                 `comment:"父级节点"`
	Name        string                `comment:"权限名称"`
	Code        string                `comment:"权限编码"`
	Type        data.EMPermissionType `comment:"权限类型"`
	Path        string                `comment:"菜单访问地址"`
	ViewPath    string                `comment:"视图路径"`
	ApiPath     string                `comment:"接口地址"`
	Icon        string                `comment:"图标"`
	Hidden      bool                  `comment:"隐藏"`
	Opened      bool                  `comment:"打开组"`
	Sort        int32                 `comment:"排序"`
	Description string                `comment:"描述"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Permission_UpdateResp struct {
	PId int32 `comment:"权限ID"`
}

// @genReq:""
// @linkResp:"Admin_ListResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"用户管理列表"
type Admin_List struct {
	PageSize int64       `comment:"每页条数"`
	Page     int64       `comment:"当前页数"`
	Filter   interface{} `comment:"查询条件"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Admin_ListResp struct {
	List      []*data.Admin `comment:"角色列表"`
	Page      int64         `comment:"当前页码"`
	PageCount int64         `comment:"总页数"`
	PageSize  int64         `comment:"每页大小"`
	Total     int64         `comment:"总数量"`
}

// @genReq:""
// @linkResp:"Admin_CreateResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"用户创建"
type Admin_Create struct {
	Email        string  `comment:"邮箱"`
	Account      string  `comment:"登陆帐号"`
	Phone        string  `comment:"手机号码"`
	Password     string  `comment:"密码"`
	Name         string  `comment:"名字"`
	Avatar       string  `comment:"头像"`
	Status       int32   `comment:"状态"`
	Remark       string  `comment:"备注"`
	AdminRoleIds []int32 `comment:"管理角色"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Admin_CreateResp struct {
	AdminId int32 `comment:"用户ID"`
}

// @genReq:""
// @linkResp:"Admin_DeleteResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"用户删除"
type Admin_Delete struct {
	AdminIds []int32 `comment:"角色ID"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Admin_DeleteResp struct{}

// @genReq:""
// @linkResp:"Admin_UpdateResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"用户更新"
type Admin_Update struct {
	AdminId      int32   `comment:"用户ID"`
	Email        string  `comment:"邮箱"`
	Account      string  `comment:"登陆帐号"`
	Phone        string  `comment:"手机号码"`
	Password     string  `comment:"密码"`
	Name         string  `comment:"名字"`
	Avatar       string  `comment:"头像"`
	Status       int32   `comment:"状态"`
	Remark       string  `comment:"备注"`
	AdminRoleIds []int32 `comment:"管理角色"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Admin_UpdateResp struct {
	AdminId int32 `comment:"角色ID"`
}

// @genReq:""
// @linkResp:"Player_RoleListResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色管理列表"
type Player_RoleList struct {
	PageSize int64       `comment:"每页条数"`
	Page     int64       `comment:"当前页数"`
	Filter   interface{} `comment:"查询条件"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Player_RoleListResp struct {
	List      []*data.GDRole `comment:"角色列表"`
	Page      int64          `comment:"当前页码"`
	PageCount int64          `comment:"总页数"`
	PageSize  int64          `comment:"每页大小"`
	Total     int64          `comment:"总数"`
}

// @genReq:""
// @linkResp:"Auth_ThirdResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"false"
// @httpPrefix:"admin"
// @comment:"飞书登录平台回调"
type Auth_Third struct {
	Code string `comment:"用户名"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"false"
type Auth_ThirdResp struct{}

// @genReq:""
// @linkResp:"Player_GetRoleResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色详情"
type Player_GetRole struct {
	RoleId int64 `comment:"角色编号"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Player_GetRoleResp struct {
	Role *data.GDRoleExt `comment:"角色信息"`
}

// @genReq:""
// @linkResp:"Account_GetSimpleConfigResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"获取服务器简单配置"
type Account_GetSimpleConfig struct {
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Account_GetSimpleConfigResp struct {
	ConfigList map[string]any
}

// @genReq:""
// @linkResp:"Resource_CheckRoleResRankResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"查询活跃玩家资源排行"
type Resource_CheckRoleResRank struct {
	CheckData *data.RoleResRank `comment:"查询信息"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Resource_CheckRoleResRankResp struct {
	Code int32 `comment:"0 查询中 1 成功 "`
}

// @genReq:""
// @linkResp:"Resource_GetResRankProcessDetailResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"获取活跃玩家资源排行详细信息"
type Resource_GetResRankProcessDetail struct {
	Id      int32 `comment:"查询唯一 Id "`
	ResType int32 `comment:"查询资源类型 "`
	ResId   int32 `comment:"查询资源类型 Id "`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Resource_GetResRankProcessDetailResp struct {
	List []*data.GDRoleMiniData `comment:"排行玩家列表"`
}

// @genReq:""
// @linkResp:"Resource_GetResRankCheckListResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"获取资源排行列表"
type Resource_GetResRankCheckList struct {
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Resource_GetResRankCheckListResp struct {
	CheckData []*data.RoleResRank `comment:"查询信息"`
}

// @genReq:""
// @linkResp:"Server_ServerListResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"请求服务器列表信息"
type Server_ServerList struct {
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Server_ServerListResp struct {
	List []*data.GDServer `comment:"服务器信息"`
}

// @genReq:""
// @linkResp:"Server_AddResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"角色管理列表"
type Server_AddServer struct {
	Param map[string]string `comment:"每页条数"`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Server_AddResp struct {
	Code int64 `comment:"是否成功 0 成功 1 失败"`
}

// @genReq:""
// @linkResp:"Player_ConfigResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"配置信息"
type Player_Config struct {
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Player_ConfigResp struct {
	Param map[string]interface{} `comment:"配置"`
}

// @genReq:""
// @linkResp:"Resource_CloseCheckResTaskResp"
// @useHttp:""
// @ignoreClient:"true"
// @customExport:"true"
// @httpPrefix:"admin"
// @comment:"关闭资源排行查询任务"
type Resource_CloseCheckResTask struct {
	Id int32 `comment:"查询唯一 Id "`
}

// @genResp:""
// @ignoreClient:"true"
// @customExport:"true"
type Resource_CloseCheckResTaskResp struct {
	Code int32 `comment:"关闭反馈"`
}
