package cmd

import "evo-data/data"

// @genReq:""
// @linkResp:"BlackStore_GoodsListResp"
// @comment:"获取商品购买数量"
type BlackStore_GoodsList struct {
	StoreId int32 `comment:"商店编号"`
}

// @genResp:""
type BlackStore_GoodsListResp struct {
	GoodsList map[int32]*data.GDBlackStoreGoods `comment:"商品购买数量"`
	Refresh   int32                             `comment:"刷新次数"`
}

// @genReq:""
// @linkResp:"BlackStore_RefreshResp"
// @comment:"刷新商品购买数量"
type BlackStore_Refresh struct {
	StoreId int32 `comment:"商店编号"`
}

// @genResp:""
type BlackStore_RefreshResp struct {
	SyncResp
	GoodsList map[int32]*data.GDBlackStoreGoods `comment:"商品购买数量"`
	Refresh   int32                             `comment:"刷新次数"`
}

// @genReq:""
// @linkResp:"BlackStore_BuyResp"
// @comment:"购买商品"
type BlackStore_Buy struct {
	GoodsId int32 `comment:"商品编号"`
}

// @genResp:""
type BlackStore_BuyResp struct {
	SyncRewardResp
	GoodsList map[int32]*data.GDBlackStoreGoods `comment:"商品购买数量"`
	Refresh   int32                             `comment:"刷新次数"`
}
