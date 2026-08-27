// Package framework 是 Bee 与 Ant 共用的私有进程装配内核。
//
// 本包持有共享基础设施并管理关闭顺序，只能由公开框架门面适配；项目代码不能直接依赖它。
package framework
