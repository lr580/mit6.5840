租期读（Lease Read）对比报告

## 对比范围与方法

- 代码开关：`src/featureflag/featureflag.go` 中 `EnableKVFastLeaseGet`。
- 快路径实现：`src/kvraft1/server.go` 的 `tryLeaseGet()`，命中后直接本地 `doGet()`，绕过 `rsm.Submit()`。
- 租期机制：`src/raft1/raft.go`，`leaderLeaseDuration = 200ms`，新 leader 在 `becomeLeader()` 后先进入 `leaseBlockedUntil` 冷却期。
- 关键测试：
  - `TestLeaseFastGetReducesRPCs`（`src/kvraft1/kvraft_test.go`）
  - `TestLeaseWaitsAfterTermSwitch`（`src/kvraft1/kvraft_test.go`）

本次额外复测（20 轮）日志：

- `lease_fast_rpc_20.log`
- `lease_term_switch_20.log`

历史全量对比日志（有/无租期读）：

- `Lab4结果有租期读.txt`
- `Lab4结果无租期读.txt`

## 结果

**读路径 RPC 明显下降**（核心收益）

基于 `TestLeaseFastGetReducesRPCs` 20 轮统计（每轮包含一次 slow 与一次 fast）：

- slow（无租期快读）平均：`46.7 RPC`，区间 `43~53`
- fast（租期快读）平均：`39.5 RPC`，区间 `36~46`
- 平均下降：`7.2 RPC/轮`，相对下降 `15.42%`

说明：该测试每轮固定做 5 次 `Get`（见 `measureLeaseRPCs()`），粗略折算约 `1.44 RPC/Get` 的下降。



全量回归下总体 RPC 也有下降（但受随机性影响）

从两份全量日志聚合（30 个 Passed 小节）：

- 有租期读：`total_rpcs=59264`，`total_time=220.7s`
- 无租期读：`total_rpcs=65859`，`total_time=226.7s`
- 变化：RPC 总量约 `-10.01%`，总耗时约 `-2.65%`

注：Lab 测试含选举与网络随机扰动，这组结果是“整体趋势”而非严格 micro-benchmark。



代价：

任期切换后存在“不可立即快读”窗口

> `TestLeaseWaitsAfterTermSwitch` 明确验证了这点：
>
> - 切换前：单次 `Get` 要求 `RPC <= 2`（快路径）
> - 新 leader 刚上任：要求 `HasLease()==false`
> - 切换后立刻读：要求 `RPC >= 3`（退回慢路径）
> - 租期重建后：再次读要求 RPC 回落（`fastRPCs < slowRPCs`）
>
> 这意味着故障切主阶段，读请求会暂时回到 Raft 提交路径，网络与延迟成本上升。



额外冷却时间（Failover 可用性代价）

代码层面（`src/raft1/raft.go`）：

- `leaderLeaseDuration = 200ms`
- 新 leader 在 `becomeLeader()` 中设置 `leaseBlockedUntil = now + leaseDuration`
- 在冷却期内 `maybeExtendLease()` 直接返回，不允许租期生效

即：任期切换后，快读至少会被延后约 `200ms` 才可能恢复（还需多数心跳响应）。



快路径并不总是“端到端更快”

同一组 20 轮 `TestLeaseFastGetReducesRPCs` 中：

- slow 场景平均时间：`0.895s`
- fast 场景平均时间：`1.03s`

原因是该测试在 fast 场景会先等待租期建立（`waitForLease`），因此虽然读 RPC 更省，但测试总时长未必更短。  
这反映的代价是：租期机制引入了“建立与恢复租期”的前置等待。

## 结论

租期读的主要优化点是**稳定期读请求减少跨节点交互**，在本仓库测试下读相关 RPC 下降约 `15.42%`。

主要代价是**故障切主后的短暂退化**：新 leader 先无租期，读会回退慢路径，并至少经历约 `200ms` 的租期冷却窗口。

因此该优化适合“读多写少、leader 稳定”的阶段；在频繁 leader 切换场景，收益会被恢复成本部分抵消。
