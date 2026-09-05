# CLAUDE.md — game-server

> 公开 repo。新会话先读这个文件。项目之外的上下文在 `CLAUDE.local.md`(不入库)。

## 这是什么

一个 **2–8 人实时同步小游戏服务器**,Go 实现,6 周(2026-09-03 → 2026-10-18)做完最小可玩闭环。
目标是一个**别人打开 README 五分钟能看懂、能跑、有压测数字**的项目,不是框架。

## Scope(定死,不许扩)

- **网关** — TCP 长连接,Protobuf 编解码,length-prefixed 解决粘包,心跳 + 超时踢人
- **登录** — token 校验,玩家会话落 Redis
- **房间 / 匹配** — 创建 / 加入 / 离开,房间内广播
- **同步** — 状态同步,服务端 20Hz tick,广播玩家位置与状态
- **持久化** — MySQL 存账号和战绩,Redis 存在线状态和房间
- **可观测** — Prometheus metrics(在线人数、tick 耗时、消息 QPS)+ 结构化日志
- **压测** — 自写 bot client,500 并发连接,产出 **P99 延迟**
- **部署** — Docker Compose 一键起

**加分项(主线做完才碰)**:帧同步对照版 + 对比文;断线重连;服务端移速校验;Unity 2D demo client(10/21 后,一个周末上限)。

**明确不做**:通用框架、插件系统、多种协议、UI。任何"顺便加一个"都先问"这在 6 周 scope 里吗"。

## 里程碑

| 周 | 时间 | 交付 |
|---|---|---|
| W1 | 9/3 – 9/13 | 工具链装好;repo 骨架 + proto 定义 + TCP echo 打通;README v1 |
| W2 | 9/14 – 9/20 | 登录 + 会话管理 + Redis 接上 |
| W3 | 9/21 – 9/27 | 房间管理 + 广播 |
| W4 | 9/28 – 10/4 | tick 循环 + 状态同步跑通,两个客户端互相看见 |
| W5 | 10/5 – 10/11 | bot client 压测 + metrics + **出数字** |
| W6 | 10/12 – 10/18 | Docker 化 + README / ARCHITECTURE / BENCHMARK / DECISIONS 定稿 |

每周 8h:工作日 09:00–10:30 ×5 + 周六 10:00–13:00。**里程碑没到不往下走,先补。**

## 目录结构

```
game-server/
├── README.md              # 一句话说明 + 架构图 + 压测数字 + 怎么跑。最重要的文件
├── docs/
│   ├── ARCHITECTURE.md    # 分层、协议设计、同步方案选型
│   ├── BENCHMARK.md       # 压测方法 + 结果 + 火焰图
│   └── DECISIONS.md       # 为什么状态同步、为什么 Go、踩过的坑
├── proto/
├── cmd/
│   ├── gameserver/
│   └── botclient/
├── internal/
│   ├── gateway/   room/   sync/   storage/   metrics/
├── deploy/docker-compose.yml
└── Makefile
```

## 工作方式

- 中文回答,English technical terms 保留原文;token 效率优先
- **每实现一块,附一段「这段为什么这么写、换一种写法会怎样」的讲解**,写进 `docs/DECISIONS.md` 或 commit message。这个项目的作者要能脱稿讲清每一行的取舍,不能只有"能跑"
- 优先标准库,第三方依赖每加一个要在 DECISIONS.md 写理由
- 涉及数字(延迟、QPS、内存)一律实测,不估
- 日期跑 `date`,不用记忆里的日期
- commit 小而频繁,message 说"为什么"

## 环境(2026-09-05 实测)

WSL2,`go` / `docker` / `protoc` **都还没装**。W1 第一步是装工具链:Go 1.23+、protoc + protoc-gen-go、Docker(WSL 里装 Docker Engine 或用 Docker Desktop 的 WSL 集成)。

## 红线

- **公开 repo**:不出现任何个人信息、学业内容、课程作业;`CLAUDE.local.md` 不入库
- 不 commit `.env`、密钥、任何真实服务器地址
- 压测数字没复现过不写进 README
