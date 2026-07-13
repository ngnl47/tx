package data

// @genData:`value:"true"`
// @comment:`value:"商品"`
type GDBlackStoreGoods struct {
	BuyQuantity int32   `comment:"购买数量" json:"buy_quantity"`
	Discount    float32 `comment:"折扣" json:"discount"`
}

// @genData:`value:"true"`
// @comment:`value:"采购折扣信息"`
type GDBlackPurchaseItem struct {
	ItemId   int32 `comment:"商品ID"`
	Discount int32 `comment:"折扣"`
}

// @genData:`value:"true"`
// @comment:`value:"黑市购买记录"`
type GDBlackPurchaseHistory struct {
	BuyMethod EMBlackStoreBuyMethod `comment:"购买方式"`
	GoodsId   int32                 `comment:"商品ID"`
	Price     int64                 `comment:"价格"`
	ItemId    int32                 `comment:"商品ID"`
	Discount  float32               `comment:"折扣"`
}

// @genEnum:`value:"true"`
// @comment:`value:"黑市购买方式"`
type EMBlackStoreBuyMethod struct {
	Default    int32 `enum:"0" comment:"手动购买"`
	Purchase   int32 `enum:"1" comment:"采购"`
	Refresh    int32 `enum:"2" comment:"刷新"`
	ManRefresh int32 `enum:"3" comment:"手动刷新"`
}
