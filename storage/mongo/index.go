package mongo

import (
	"github.com/ngnl47/tx/meta"
	"github.com/ngnl47/tx/storage/mongoindex"
)

// IndexKey 描述已经解析为 BSON 路径的一段 Mongo 索引键。
type IndexKey = mongoindex.IndexKey

// IndexWeight 描述已经解析为 BSON 路径的文本索引权重。
type IndexWeight = mongoindex.IndexWeight

// IndexModel 是交给具体 Mongo driver 的后端无关索引定义。
type IndexModel = mongoindex.IndexModel

// BuildIndexModels 按字段声明顺序聚合同一个类型的命名和匿名索引。
func BuildIndexModels(spec meta.TypeSpec) ([]IndexModel, error) {
	return mongoindex.BuildIndexModels(spec)
}
