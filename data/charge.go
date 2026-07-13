package data

// @genEnum:`value:"true"`
// @comment:`value:"充值渠道"`
type EMChargeType struct {
	BuyDailyDeal       int32 `enum:"1" comment:"购买每日特惠"`
	BuyShopCard        int32 `enum:"2" comment:"购买月卡"`
	BuyRegularGiftPack int32 `enum:"3" comment:"购买普通礼包"`
	BuyFund            int32 `enum:"4" comment:"购买基金"`
	BuyFundTask        int32 `enum:"5" comment:"购买基金任务"`
	BuyDiamondShop     int32 `enum:"6" comment:"购买钻石商城"`
	BuyNewbie          int32 `enum:"7" comment:"购买新手礼包"`
	BuyTechSlot        int32 `enum:"8" comment:"购买科技升级槽位"`
	BuyEquipGift       int32 `enum:"9" comment:"购买神兵礼包"`
	BuyResurrectPack   int32 `enum:"10" comment:"购买复活礼包"`
	SeasonPass         int32 `enum:"11" comment:"赛季通行证"`
	SignInPass         int32 `enum:"12" comment:"签到通行证"`
	ActNewbie          int32 `enum:"13" comment:"群雄并起活动"`
	ActBestPlayer      int32 `enum:"14" comment:"最强领主活动"`
	VipGift            int32 `enum:"15" comment:"vip礼包"`
	MonthCard          int32 `enum:"16" comment:"月卡"`
}

// @genEnum:`value:"true"`
// @comment:`value:"充值渠道"`
type ChargeChannel struct {
	Default int32 `enum:"0" comment:"默认"`
	Dy      int32 `enum:"1" comment:"抖音"`
}
