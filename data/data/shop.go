package data

// @genData:`value:"true"`
// @comment:`value:"DailyDeal"`
type GDShopDailyDeal struct {
	BuyCount map[int32]int32
}

// @genData:`value:"true"`
// @comment:`value:"商城信息"`
type GDShop struct {
	GDShopDailyDeal GDShopDailyDeal
}

// @genData:`value:"true"`
// @comment:`value:"普通礼包"`
type GDRegularGiftInfo struct {
	IdRecord    map[int32]int32 `comment:"礼包编号key,id,,value,次数"`
	LastBuyTime int64           `comment:"最后购买时间"`
}

// @genData:`value:"true"`
// @comment:`value:"普通礼包"`
type GDRegularGift struct {
	ThemeRecord  map[int32]*GDRegularGiftInfo `comment:"连续礼包记录"`
	NewBieRecord map[int32]int32              `comment:"新手礼包记录"`
}
