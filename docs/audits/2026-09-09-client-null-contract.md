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

以上为修复前复现结果。

## 可空值修复验收

- 生成器保留 Go 指针、切片及 map 的可空类型，schema 以 nullable 标记表达，不修改 Go 输入和服务端编解码。
- 两个独立客户端分别在 BON 编解码及 JSON 表单转换中保留 null；固定数组和非可空对象仍拒绝 null。
- 同一探针重新生成 schema 后，两客户端均正确解码上述四类 null；客户端将这些值重新编码，字节与 Go 原始结果逐字节一致。
- 既有生成测试增加指针标量、二进制切片、固定数组和可空容器元素断言；客户端每工程增加一个可空值测试，另复用表单测试验证 null 不被转换。没有枚举任意深度组合。
- txgen 全量 Go 测试通过；txclient 77 项、txclientslg 97 项测试及两客户端构建通过。两项目生成入口分别执行，服务端 Go 产物无变化；本轮未重跑真实集群验收。
- 可空值与字段缺省分别实施，不把 undefined 自动转成 null。

## 字段缺省修复验收

核对实际 Go 标签生成路径：data 使用 fieldTag，cmd 及辅助结构使用 cmdFieldTag，两者最终都通过 serializedFieldTag 输出 `字段名,omitempty`。因此不能仅按输入是否显式带 omitempty 判断；当前所有客户端对象字段均应为可选字段，枚举和路由描述符不在该范围内。

两个 TypeScript 模板直接输出可选字段，不增加配置、标签解析器或公共元数据。两客户端 JSON 表单跳过 undefined，保留明确提供的 0、false、空容器和 null。已有 BON 编解码已保留缺省，不需要新增编码规则。此保证属于载荷与客户端对象层，不声称普通 Go struct 解码后还能区分缺省标量和零值。

复用了生成器类型测试和客户端表单、编解码测试：先复现必填类型与表单缺省报错，再验证 `{}` 和 `{count:0}` 编码不同、解码结果分别保留。txgen 全量测试、txclient 77 项和 txclientslg 97 项测试及构建均通过。两套示例分别再生成，Go 产物与 CommandID 清单无变化。未新增测试函数，未重跑真实集群。
