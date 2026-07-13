package data

// @genData:`value:"true"`
// @storage:`mode:"fusion" delaySaveTime:"600" delayDelTime:"86400"`
// @redis:`routeType:"role" routeBy:"roleId" idField:"roleId" key:"roleExtData"`
// @mongodb:`routeType:"role" routeBy:"roleId" idField:"roleId" collection:"rolesExtData" enableSharding:"true"`
// @comment:`value:"角色"`
type GDRoleExtData struct {
	RoleId int64            `comment:"角色编号" mongodb:"index,unique"`
	Patch  map[string]int64 `comment:"补丁标记"`
}
