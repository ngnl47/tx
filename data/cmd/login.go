package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"Login_ManifestResp"
// @useHttp:""
type Login_Manifest struct {
	Platform string `comment:"平台,debug,hortor"`
	Version  string `comment:"客户端版本"`
}

// @genResp:""
type Login_ManifestResp struct {
	IsLast           bool             `comment:"是否是最新"`
	BattleVersion    int32            `comment:"战斗版本号"`
	BundleVers       string           `comment:"客户端"`
	DataVers         string           `comment:"版本号"`
	DataBundleVer    string           `comment:"配置版本号"`
	ServerUrl        string           `comment:"服务器地址"`
	Config           map[string]int64 `comment:"动态配置"`
	VipQRCodeList    []string         `comment:"VIP 二维码图片列表"`
	RabbitQRCodeList []string         `comment:"秘兔二维码图片列表"`
}

// @genReq:""
// @linkResp:"Login_AuthUserResp"
// @useHttp:""
// @comment:"授权登录"
type Login_AuthUser struct {
	Platform       string `comment:"平台,debug,hortor"`
	PlatformExt    string `comment:"平台,debug,hortor"`
	OriPlatform    string `comment:"原始平台"`
	Info           string `comment:"平台认证信息,包含加密的用户信息"`
	ServerId       int64  `comment:"服务器编号"`
	Scene          int32  `comment:"微信小程序场景"`
	ReferrerInfo   string `comment:"微信小程序跳转来源信息"`
	DeviceUniqueId string `comment:"设备id"`
}

// @genResp:""
type Login_AuthUserResp struct {
	RoleToken string
	RoleId    int64
}

// @genReq:""
// @linkResp:"Login_AuthUserResp"
// @useHttp:""
// @comment:"超级授权"
type Login_SuperAuthUser struct {
	Password string `comment:"超级密码"`
	RoleId   int64  `comment:"角色编号"`
}

// @genReq:""
// @linkResp:"Login_ServerListResp"
// @useHttp:""
// @comment:"获取服务器列表"
type Login_ServerList struct {
	Platform    string `comment:"平台,debug,hortor"`
	PlatformExt string `comment:"平台,debug,hortor"`
	OriPlatform string `comment:"原始平台"`
	Info        string `comment:"平台认证信息,包含加密的用户信息"`
	AreaId      int64  `comment:"大区"`
}

// @genResp:""
type Login_ServerListResp struct {
	AreaList     []*data.GDArea               `comment:"大区列表"`
	ServerList   []*data.GDServer             `comment:"服务器列表"`
	RoleCount    map[string]string            `comment:"角色数量"`
	RecommendId  int64                        `comment:"推荐服务器"`
	Roles        map[int64]*data.GDServerRole `comment:"拥有角色服务器"`
	DeletedRoles map[int64]*data.GDServerRole `comment:"拥有被删除的角色服务器"`
	MaxViewId    int64                        `comment:"最大的服务器编号"`
}

// @genReq:""
// @linkResp:"Login_DataVersionResp"
// @useHttp:""
// @comment:"获取配置版本号"
type Login_DataVersion struct{}

// @genResp:""
type Login_DataVersionResp struct {
	DataVers string `comment:"版本号"`
}

// @genReq:""
// @linkResp:"Login_UpdateVersionResp"
// @useHttp:""
// @comment:"获取服务器列表"
type Login_UpdateVersion struct {
	Password    string `comment:"密钥"`
	Platform    string `comment:"平台,debug,hortor"`
	OriPlatform string `comment:"原始平台"`
	Version     string `comment:"客户端版本"`
	ServerUrl   string `comment:"跳转地址"`
	BundleVers  string `comment:"Bundle信息"`
}

// @genResp:""
type Login_UpdateVersionResp struct {
}

// @genReq:""
// @useHttp:""
// @comment:"上传manifest"
type UploadManifest struct {
	Token         string `comment:"令牌"`
	Version       string `comment:"版本号"`
	Platform      string `comment:"平台类型"`
	BundleVersion string `comment:"资源版本号"`
	DataVersion   string `comment:"数据版本号"`
}

