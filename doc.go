// Package tx 提供 Tx 的根包入口。
//
// 该包保持轻量，只用于让模块根路径 github.com/ngnl47/tx
// 成为可导入的 Go package。具体能力仍按职责拆分在子包中：
// anno 定义注解，astscan 扫描源码，meta 描述元数据，storage 提供存储运行时。
package tx

// Version 表示当前开发版本。
const Version = "v0.0.0-dev"
