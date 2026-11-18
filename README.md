# warm-standby-failover
Python 温备集群切换库，处理复制滞后、fencing、提升顺序和有限数据损失

## 设计

`warmstandby.FailoverCoordinator` 为一个主备对驱动状态机：

```
STEADY -> SUSPECT -> FENCING -> PROMOTING -> FAILED_OVER
                          \__________ ROLLED_BACK (超时 / 令牌丢失)
```

提升顺序保证安全：

1. **探测**：主心跳超过 `heartbeat_timeout` 进入 SUSPECT，再经过一个
   宽限期才评估提升，避免抖动误切换。
2. **Fencing 优先**：先在 `FenceTokenStore`（CAS 令牌，etcd/ZK 语义）
   中为备机取令牌，旧主被逐出即完成 fencing；拿不到令牌绝不提升，
   杜绝脑裂。
3. **损失预算**：比较旧主最后持久化位点与备机 flush 位点，
   超出 `max_allowed_loss` 则拒绝提升并返回损失窗口。
4. **提交前再校验**：`confirm_promotion` 前重新检查令牌仍归自己，
   竞态（双向分区两侧同时切换）下只有一方能提交。
5. **Epoch 写入门控**：切换后 epoch+1、路由版本 +1；
   `accept_write` 只接受当前 epoch 且非切换中的写入，
   存储侧 `storage_write_permitted` 再挡掉被 fencing 的旧主。
6. **超时回滚**：`promote_timeout` 内未收到备机确认则回滚路由、
   epoch 与 fencing 令牌。

切换结果通过 `LossWindow` 给出：

- `recoverable_lsn`：可恢复位点（备机已持久化的最高位点），客户端
  可从此位点重放/对账。
- `primary_durable_lsn`：旧主最后已知持久化位点，两者之差即
  **数据损失窗口**——这部分请求曾对旧主客户端返回成功，但在新主上
  不存在，需要业务层补偿。

旧主恢复后调用 `rejoin()`，返回 `RejoinReport`：必须以备机身份加入、
采用新 epoch、回退到 `must_rewind_to`，其 `divergent_window`
即旧主上已确认但丢失的分叉写入。

## CAP 取舍

本库选择 **CP**：分区发生时宁可拒绝服务也不允许双主。

- **一致性优先**：fencing 未确认前绝不提升；提升提交前二次校验令牌；
  切换期间所有写入被拒绝（客户端重试），旧 epoch 写入一律拒绝。
  代价是切换窗口内可用性下降。
- **显式的损失边界**：温备复制天然滞后，故障时"返回成功但备端不存在"
  的写入不可避免。库不掩盖这一点，而是用 `max_allowed_loss` 让运维
  显式声明 RPO 预算，超出即拒绝提升（等待人工或 `force`），
  并把损失窗口和可恢复位点作为一等结果返回。
- **分区下的可用性**：双向分区时只有能拿到 fencing 令牌的一侧可推进，
  另一侧保持只读/拒绝写——牺牲 A 保住 C；网络恢复后通过 `rejoin()`
  收敛分叉。

## 运行测试

```
python -m unittest
```

覆盖：双向分区单胜者、旧主恢复分叉报告、复制滞后拒绝/放行、
提升超时回滚、回滚后路由与 epoch 还原、切换期写入门控。
