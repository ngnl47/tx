# 客户端空值与字段缺省契约复核

## 范围与方法

使用临时 Go 输入，经 txgen.GenerateTypeScript 生成类型和 schema；使用 tx 的 bon.MarshalEnvelope 编码合法 Go 值，再分别调用 txclient、txclientslg 的真实 decodeCommand。未修改生成产物或业务数据定义，未新增常驻测试。

临时探针目录：`/private/tmp/tx-null-contract.ZhMwqe`。该目录不是长期依赖；以下输入与结果可用于重建回归。

输入响应 Probe 包含 `Item *Item`、`Items []*Item`、`Values map[string]*Item`，三个字段无 omitempty；另有 `Count int32` 使用 `msgpack:"count,omitempty"`。Item 同样有可省略的 Count 字段。

## 实测结果

两客户端结果一致：

| Go 值 | BON 语义 | 客户端结果 |
| --- | --- | --- |
| Item=nil | null | Item 解码结果不是对象 |
| Items=nil | null | 协议数组字段解码结果不是 Array |
| Values=nil | null | 协议 map 字段解码结果不是文档 |
| Items 包含 nil 指针 | 数组元素为 null | Item 解码结果不是对象 |
| 非 nil 空容器、Count=0 | 容器保留，Count 缺省 | 解码成功，Count 不存在 |

生成类型却是 `item: Item`、`items: Array<Item>`、`values: Map<string, Item>`、`count: number`。因此空值是运行时拒绝问题，字段缺省是静态类型与实际值不一致问题，不能混为一个解码异常。

## 原因与最小修复方向

1. txgen 的 TypeScript 类型与 schema 构建递归剥掉指针；切片和 map 也没有表达 null。客户端只有基础类型、对象、数组和 map 分支，对这些值执行非空校验。
2. 生成字段没有表达 omitempty，而解码器仅写入实际存在的字段。不能通过填零解决，因为缺省与显式零值可能具有不同业务含义。
3. 先补空值契约：由 Go 类型推导可空信息，生成客户端可空类型与 schema；两个客户端分别支持可空值的编码和解码。保留固定数组与切片的差异，不放宽非可空标量，不把 null 转为空容器。
4. 再补字段缺省契约：依据有效序列化标签标记可选字段，保留缺省，不给 Go 输入增加 TypeScript 专属注解。具体读取哪一层标签元数据需实施前核对，不能只读取原始注解而忽略生成标签默认值。
5. 两组示例分别重新生成、验证；不交叉引用。正式回归优先复用既有生成器及客户端编解码测试，仅增加上述关键状态。

本轮仅完成复现与范围评估，未修复生产代码。未验证任意嵌套组合、客户端空值上行至 Go、二进制切片及固定数组边界；这些属于后续最小修复的验收范围，不标记为本轮通过。
