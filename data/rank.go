package data

// @genData:`value:"true"`
// @comment:`value:"段位分排名"`
type GDRankLBScoreMember struct {
	RoleId      int64   `comment:"好友编号"`
	Name        string  `comment:"玩家昵称"`
	HeadImg     string  `comment:"玩家头像"`
	RankLevel   int32   `comment:"段位"`
	Rank        int32   `comment:"排名"`
	Score       float64 `comment:"排名分数"`
	AvatarFrame int32   `comment:"头像框"`
	PlayerCard  int32   `comment:"vip卡片"`
}

// @genData:`value:"true"`
// @comment:`value:"主线关卡排名"`
type GDRankBattleLevelMember struct {
	RoleId      int64   `comment:"好友编号"`
	Name        string  `comment:"玩家昵称"`
	HeadImg     string  `comment:"玩家头像"`
	BattleLevel int32   `comment:"主线编号"`
	Rank        int32   `comment:"排名"`
	Score       float64 `comment:"排名分数"`
}
