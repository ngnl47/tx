// Package anno 定义供 AST 扫描器使用的可扩展注解模型。
//
// 它不关心注解会被 storage、生成器或运行时代码如何消费。调用方通过
// Definitions 描述本次希望识别的注解，再把这些定义传给扫描器。
//
// type 级注解以整行形式写在结构体注释区：
//
//	// @custom
//	// @custom:``
//	// @custom:`owner:"roleId" ttl:"600"`
//	type Item struct {}
//
// field 级注解使用普通 Go struct tag：
//
//	type Item struct {
//		RoleID int64 `customField:"name:owner"`
//	}
//
// 两种形式都会被解析为 Directive。解析阶段所有值都保持字符串；下游代码
// 可以根据自己提供的 Definition 再把它们转换为 int、bool、time.Duration
// 或领域内自定义类型。
package anno
