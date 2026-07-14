package cmd

// @genReq:""
// @linkResp:"SyncRewardResp"
type Item_BuyBatchItem struct {
	ItemId int32
	Count  int64
}

// @genReq:""
// @linkResp:"SyncRewardResp"
type Item_OpenPack struct {
	ItemId int32 `comment:"物品编号"`
	Number int32 `comment:"打开数量"`
	Index  int32 `comment:"选择礼包内容"`
}

// @genReq:""
// @linkResp:"SyncRewardResp"
type Item_UseItem struct {
	ItemId    int32   `comment:"物品编号"`
	Number    int32   `comment:"使用数量"`
	IntParams []int32 `comment:"附加参数列表"`
}
