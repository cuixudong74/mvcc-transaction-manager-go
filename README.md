# MVCC Transaction Manager (Go)

一个仅在本地进程内运行的事务型多版本键值存储。数据通过 CRC 保护的追加式 WAL 落到本地目录，不依赖数据库、网络服务或后台守护进程。

## 快速开始

```go
package main

import (
    "fmt"
    "log"

    "github.com/cuixudong74/mvcc-transaction-manager-go/engine"
)

func main() {
    db, err := engine.Open(engine.Options{
        Directory:  "./data",
        SyncWrites: true, // 生产环境建议开启
    })
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    txn, err := db.Begin(false) // false = read-write
    if err != nil {
        log.Fatal(err)
    }
    if err := txn.Put("user:1", []byte("alice")); err != nil {
        log.Fatal(err)
    }
    if err := txn.Commit(); err != nil {
        log.Fatal(err)
    }

    readTxn, _ := db.Begin(true) // true = read-only
    defer readTxn.Rollback()
    value, ok, err := readTxn.Get("user:1")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(string(value), ok)
}
```

范围扫描使用字典序的 `[Start, End)`：

```go
pairs, err := txn.Scan(engine.Range{Start: "user:", End: "user;"})
```

空 `Start` 表示从最小键开始，空 `End` 表示扫描到最大键。`Limit > 0` 时按排序顺序截断。

## 隔离语义

- `Begin(readOnly)` 返回一个一致快照，快照时间为该事务开始前最后一个已发布提交。
- 事务只能看到：
  1. 提交时间戳不晚于自身快照时间的版本；
  2. 自己在当前事务内写入的待提交值。
- 其他事务随后提交的版本不会改变该事务的 `Get` 或 `Scan` 结果。
- 未提交数据只保存在事务内存中，其他事务永远无法读取。
- `Scan` 在同一把状态锁下收集并解释键集合，因此范围结果来自单个快照；不会把不同时刻、不同事务的字段或键混合。
- 只读事务只短暂持有读锁来读取不可变版本链。提交期间的磁盘 fsync 和 GC 的临时文件写入不持有该锁，所以进行中的只读事务不会被这些 I/O 阻塞。

## 写写冲突策略

引擎采用 **快照隔离 + first-committer-wins**：

1. 两个读写事务在重叠事务区间内修改同一个键。
2. 先通过提交校验的事务分配下一个提交时间戳并成功。
3. 后到事务发现该键已有版本晚于自己的快照时间，返回 `engine.ErrConflict`。
4. 失败事务的所有写入均不发布，不产生部分可见修改，也不会写入版本链。
5. 调用方应重新 `Begin`，在新快照上读取并重试；重试结果等价于串行执行。

冲突判定只比较同一键上的已提交版本。快照隔离不额外要求谓词锁或幻读防护。

## 多版本链与回收规则

每个键维护一条“新到旧”的不可变版本链。版本包含：

- 事务 ID；
- 单调递增的提交时间戳；
- 值或删除墓碑。

`RunGC()`：

1. 与提交共用 `commitMu`，保证不会观察到两个不同的提交顺序。
2. 计算安全快照时间：所有活跃事务中最早的 `readTS`；没有活跃事务时为最新已提交时间。
3. 保留任何仍可能被活跃事务读取的版本。
4. 删除严格早于安全点、且不再需要用于判定活跃事务冲突的旧版本。
5. 若删除墓碑本身已经被所有当前及未来快照越过，则整条链删除。
6. 将保留状态编码为一个带 CRC 的权威 snapshot 记录，写入同目录唯一临时文件，`fsync` 后原子 `rename` 为 `mvcc.db`，再 fsync 目录。
7. 替换日志的文件 I/O 期间不持有内存状态读锁，因此不阻塞进行中的只读扫描。

只要存在长事务，GC 就保留其快照仍需的版本。长事务结束后再次调用 `RunGC` 才会回收这些历史版本。

## 提交、回滚与崩溃恢复

WAL 记录格式为：

```text
4-byte length | 4-byte CRC32(JSON) | JSON payload
```

提交协议：

1. 事务写入期间，`Put/Delete` 只修改进程内 pending map，不写未提交数据。
2. `Commit` 获得提交串行锁并完成冲突校验。
3. 校验通过后，一次性顺序写入 `begin`、所有 mutation 和最终 `commit` 记录。
4. `commit` 记录是唯一可见性决策：
   - 完整存在：恢复时发布该事务；
   - 缺失或最后帧损坏：此前的 `begin/mutation` 一律忽略。
5. 重启时遇到损坏的最后一帧会丢弃该帧及其之后内容，并将文件截断到最后一个完整帧。
6. GC 原子重命名前后的崩溃由临时文件清理和 snapshot 重放处理；不会出现一半版本来自旧日志、一半来自新 snapshot 的状态。

显式 `Rollback` 只丢弃内存 pending map，因为未提交修改从未进入 WAL。

`SyncWrites: true` 时每条提交记录 fsync；`false` 只用于让单元测试快速运行，不等价于生产环境的断电持久性保证。若 fsync 返回未知 I/O 错误，引擎进入 fatal/closed 状态，调用方必须重新 `Open`，按崩溃恢复路径处理。

## 可复现性

确定性测试用固定随机种子生成相同操作序列，并在两个独立数据目录中重放。序列包含：

- 多键覆盖写和删除；
- 提交与回滚；
- 中途关闭并重新打开；
- 多次 GC；
- 活跃事务因进程退出而丢失。

测试逐条比较最终 KV、版本统计、累计 GC 统计以及压缩后的 WAL 字节，确保相同种子和初始状态产生相同结果。

## 测试

```sh
go test ./...
go test -race ./...
go vet ./...
```

测试覆盖：

- 快照隔离与自己读自己写；
- 并发读写和稳定只读事务；
- first-committer-wins 与冲突后重试；
- 回滚后的不可见性；
- 范围扫描快照自洽；
- 长事务与并发 GC；
- 墓碑回收；
- WAL torn tail、重启和压缩临时文件恢复；
- 固定随机种子的双目录确定性重放。
