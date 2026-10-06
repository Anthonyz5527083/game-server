# BENCHMARK

数字一律实测。每组数字都写明环境、命令和跑了几次;没复现过的不写。

## 环境

| 项 | 值 |
|---|---|
| CPU | Intel Core i5-14600KF,20 个逻辑核 |
| 内存 | 15 GiB |
| 系统 | WSL2,内核 6.18.40.1-microsoft-standard-WSL2 |
| Go | go1.27.1 linux/amd64 |
| `ulimit -n` | 1048576 |
| 测量日期 | 2026-10-06 |

WSL2 上的数字只对这台机器有效。换机器先重跑。

## 微基准(`go test -bench`)

### 编解码(B1.1 / B1.2)

```
go test -run '^$' -bench . -benchmem -count 5 ./internal/gateway
```

| 基准 | ns/op(5 次) | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkMarshalPing`:编码一帧 Ping(含长度头) | 133.3–136.8 | 16 | 1 |
| `BenchmarkReadEnvelopePing`:从 bufio.Reader 读一帧并解码 | 197.3–198.7 | 136 | 5 |

编码只分配 1 次,是 `proto.Size` + `MarshalAppend` 到预留容量的 slice 的效果(D2)。

### 房间广播(B3.1 / B3.2)

```
go test -run '^$' -bench Broadcast -benchmem -count 5 ./internal/room
go test -run '^$' -bench QuickMatchLeave -benchmem -count 3 -cpu 1,4,20 ./internal/room
```

| 基准 | ns/op | B/op | allocs/op |
|---|---|---|---|
| `BenchmarkBroadcast/members=2`:一人加入 + 离开(两次广播) | 676.8–678.6 | 520 | 13 |
| `BenchmarkBroadcast/members=8` | 852.0–856.4 | 688 | 13 |
| `BenchmarkQuickMatchLeave`,`-cpu 1` | 1629–1671 | 343–346 | 11 |
| `BenchmarkQuickMatchLeave`,`-cpu 4` | 922.7–930.8 | 522–523 | 12 |
| `BenchmarkQuickMatchLeave`,`-cpu 20` | 968.7–974.8 | 640–642 | 12 |

- 2 人房和 8 人房的分配次数相同:广播只编码一次,所有人共享同一个 `[]byte`(D14)。字节数随成员数涨,是房间快照里的成员列表。
- 一把锁下,4 核和 20 核的吞吐基本一样(每对操作约 0.92–0.97 µs,即每秒约 100 万次进出),说明这时瓶颈就是那把锁。需求是每秒几十次,所以这把锁不是问题(D12)。

## 冒烟:loopback 串行 RTT(B1.3)

不是压测。单连接,发一个 Ping、等到 Pong 再发下一个,量的是「一次往返最少要多久」。

```
make build
./bin/gameserver -addr 127.0.0.1:17777 -log-level warn &
./bin/botclient -addr 127.0.0.1:17777 -player 1 -n 5000 -interval 1ms -q   # 跑 3 次
```

| 次 | min | p50 | p99 | max |
|---|---|---|---|---|
| 1 | 110.1 µs | 176.0 µs | 410.2 µs | 1.79 ms |
| 2 | 107.6 µs | 176.9 µs | 510.7 µs | 7.21 ms |
| 3 | 105.1 µs | 177.1 µs | 454.3 µs | 2.39 ms |

分位数用最近秩:`sorted[ceil(p·n) − 1]`。每次 5000 个样本,p99 由第 4950 个样本决定。

## 压测(Lab 5)

尚未测量。负载模型先定下来,和实验手册里的基线原型一致才能对比:

- 500 条连接,快速匹配进 63 个房间(62 × 8 + 1 × 4)
- 每个 bot 每秒 20 个移动输入 + 5 个 Ping;Ping 按 ticker 发,不等上一个 Pong(避免 coordinated omission)
- 预热 5 秒,测量 30 秒,同一配置跑 3 次;bot 和服务端分两个进程
- 报 Ping RTT 的 p50 / p99 / max、快照间隔的 p50 / p99 / max、快照到达率、服务端 tick 耗时
