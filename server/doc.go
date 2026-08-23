// Package server 使用强类型配置、生成数据绑定和可独立选择的服务装配一个游戏服务进程。
//
// Server 持有共享基础设施并管理关闭顺序。项目代码仍负责解析 YAML、读取部署身份、
// 注册业务处理器，以及暴露 db.Save 等精简 facade。
package server
