package cmd

import "evo-data/data"

// --- 派遣寻宝 Dispatch ---

// @genReq:""
// @linkResp:"Dispatch_QueryResp"
// @comment:"查询派遣寻宝"
type Dispatch_Query struct{}

// @genResp:""
// @comment:"查询派遣寻宝返回"
type Dispatch_QueryResp struct {
	Level int32                      `comment:"派遣等级"`
	List  []*data.GDDispatchTaskItem `comment:"任务列表"`
}

// @genReq:""
// @linkResp:"Dispatch_StartResp"
// @comment:"开始派遣"
type Dispatch_Start struct {
	TaskIds []int64   `comment:"任务id列表（唯一）"`
	HeroIds [][]int32 `comment:"武将id列表，固定2个"`
}

// @genResp:""
// @comment:"开始派遣返回"
type Dispatch_StartResp struct {
	SyncResp
}

// @genReq:""
// @linkResp:"Dispatch_CompleteResp"
// @comment:"完成派遣（领取奖励）"
type Dispatch_Complete struct {
	TaskIds []int64 `comment:"任务id列表（唯一）"`
}

// @genResp:""
// @comment:"完成派遣返回"
type Dispatch_CompleteResp struct {
	SyncResp
	Reward             []*data.GDRewardData `comment:"本次奖励"`
	ExtraReward        []*data.GDRewardData `comment:"任务品质/星级满足时的额外奖励，用于特殊展示"`
	LegionBoxTriggered bool                 `comment:"是否触发军团宝箱"`
}
