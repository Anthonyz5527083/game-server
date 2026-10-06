# DECISIONS

每个设计决定一条,编号和实验手册的 D 编号一致。每条回答三个问题:

- **为什么是这个**
- **为什么不是别的**(别的写法会怎样)
- **什么时候会换**(什么条件下这个决定不再成立)

---

## Lab 1 · 网关

### D0 语言:Go

**决定**:服务端和 bot 都用 Go。网关层只用标准库 + protobuf 运行时。

**为什么是这个**
- `net` / `bufio` / `encoding/binary` / `log/slog` / `context` 覆盖了网关层的全部需求,不用选第三方网络框架。
- goroutine-per-connection 跟「每个客户端一条长连接、彼此独立」的模型天然对应:每条连接就是一段顺序代码,不用手写状态机。
- 编译成单个静态二进制,Docker 运行镜像可以很小。

**为什么不是别的**
- C++:性能上限更高,但 epoll / Reactor、内存管理、构建系统都要自己搭,6 周做不到同样的完成度和测试覆盖。
- Java + Netty:生态成熟,但这个项目要讲清楚的是连接模型和调度,Netty 把这层封装掉了。

**什么时候会换**:单进程扛不住、要榨每一个 CPU 周期(比如 MMO 的 AOI 计算)时,热点模块换 C++。这个项目的规模(500 连接、8 人房)远到不了那一步。

**要能讲清的一点**:goroutine-per-connection 底层仍然是 epoll(runtime 的 netpoller)。区别是 Go 把「事件回调」变成了「阻塞式的顺序代码」,复杂度从业务代码里的状态机挪到了 runtime 调度器里。

---

### D1 消息类型放在 Envelope 的 oneof 里,不放帧头

**决定**:帧 body 永远是 `Envelope{seq, oneof payload}`,新消息只往 oneof 加字段。字段号按模块分段:10–19 网关,20–29 登录,30–39 房间,40–49 同步。

**为什么是这个**
- 一次 `proto.Unmarshal` 同时拿到类型和内容,Go 里 `switch env.GetPayload().(type)` 是类型安全的。
- 协议目录就是 `.proto` 文件本身,不用另外维护「type id → message」的注册表,两端不会对不上。
- 字段号分段,看 diff 就知道改的是哪个模块。

**为什么不是别的**:帧头放 `uint16 msg_type` + 裸 payload,能省 Envelope 的几个字节和一次很小的解码,但要手工维护注册表,两端各写一份 switch。省下的开销在本项目的量级下测不出来(B1.2:解码一帧 Ping 约 200 ns)。

**什么时候会换**:Lab 5 的火焰图里 Envelope 解码占比显著时;或者网关要做「不解 body 就转发」的纯路由层时,type id 放帧头才有意义。

`seq` 和 `client_time_ms` 看着重复,职责不同:`seq` 用来配对请求和应答(任何消息都能用);Ping 的 `client_time_ms` 由 Pong 原样带回,客户端不保存任何状态就能算 RTT,bot 开 500 连接时省掉 500 张表。

---

### D2 帧格式:4 字节大端长度前缀,上限 64 KiB

**决定**:`[len: uint32 BE][body]`,len 只计 body。body 为 0 字节是协议错误;超过 64 KiB 直接断开。实现在 `internal/gateway/codec.go`。

**为什么是这个**
- TCP 是字节流,没有消息边界(本机实测:两条消息一次 `Read` 拿到;200000 字节被拆成 53 次 `Read`)。长度前缀是最简单的定界方法:读 4 字节就知道还要读多少,O(1)。
- 4 字节不是 2 字节:2 字节的上限正好 64 KiB,和 MaxFrameSize 撞在一起没有余量。多 2 字节对一帧 Ping(15 字节)是 13%,对 8 人快照可以忽略。
- 大端是网络字节序的惯例,抓包工具默认按大端显示。
- 上限是安全措施:没有它,对端发一个 `0xFFFFFFFF` 就能让服务端分配 4 GiB。校验写在 `make` 之前,有测试盯着(`TestReadFrameRejectsBeforeAllocating`)。
- 零长度帧算错:合法的 Envelope 至少有一个 payload,body 不可能是 0 字节。

**为什么不是别的**
- 分隔符(比如 `\n`):protobuf 是二进制,body 里什么字节都可能出现,要转义,又慢又容易错。
- 定长帧:Ping 十几字节、快照上百字节,定长会浪费带宽。
- varint 长度(protobuf 的 delimited 写法):省 1–3 字节,但必须逐字节读长度,代码更绕,收益在这个量级看不见。

**什么时候会换**:要传大块数据(回放文件、地图)时,不是改上限,而是分块传输;要跟别的语言的客户端对接时,格式不变,只要两边对齐。

---

### D3 生成的 `.pb.go` 入库

**决定**:`internal/pb/game.pb.go` 跟 `.proto` 一起 commit。改 `.proto` 后跑 `make proto`。

**为什么是这个**:别人 clone 下来不装 protoc 也能 `go build`,README 的「怎么跑」少一步。生成代码的 diff 也能在 review 里看到协议改动的实际效果。

**为什么不是别的**:不入库的话,每个使用者和 CI 都要装同版本的 protoc 和 protoc-gen-go,版本不一致还会生成不同的代码。

**什么时候会换**:proto 文件多到生成代码的 diff 淹没 review 时,改成 CI 里生成 + 检查「生成结果和入库的一致」。

---

### D4 每连接读 / 写两个 goroutine,写走有界队列,队列满就断开

**决定**:`readLoop` 独占读端,`writeLoop` 独占写端。`Conn.Send` 只是往发送队列(默认 256 帧)投递,不阻塞;队列满了就断开这条连接。实现在 `internal/gateway/conn.go`。

**为什么是这个**
- 写只发生在一个 goroutine 里,所以 `net.Conn` 不用加锁,两帧的字节也不会交错。
- 广播方(房间、tick 循环)调 Send 永远不会阻塞,一个慢客户端拖不住整个房间。
- 队列满说明客户端已经落后几秒:256 帧在 20Hz 下是 12.8 秒的快照。这种客户端留着也没法玩,不如断开让它重连。

**为什么不是别的**
- 直接在调用方 goroutine 里 Write:`net.Conn` 的并发 Write 是安全的,但会阻塞;对端不读时,广播循环就卡在这个人身上,房间里其他 7 个人一起卡。
- 队列满时阻塞:同上,慢客户端拖慢所有人。
- 队列满时丢这一帧:对快照来说可以接受(下一帧会覆盖),但对请求 / 应答不安全,客户端会永远等不到应答。

**什么时候会换**:Lab 4 有了快照之后,可以考虑分两类:快照允许丢旧帧(只保留最新的一帧),应答不允许丢。那时这里要拆成两个队列,或者给帧打「可丢」标记。

**相关的两个细节**
- `SendAndClose`:踢人时先把原因帧写出去再断。实现是往队列里放一个带 `closeAfter` 标记的帧,writeLoop 写完它就断开;它前面排队的帧照常先写(FIFO)。直接 Close 的话,客户端只看到连接断了,不知道为什么。
- `Handler` 的 OnOpen / OnMessage / OnClose 都在这条连接的读 goroutine 上按顺序调用。连接可能被别的 goroutine 关掉(比如广播时发现队列满),但 OnClose 不在那个 goroutine 里跑,而是读 goroutine 被唤醒后再跑。好处是上层处理同一条连接时不用担心「消息和断线并发」,也不会出现「广播方持锁 → 触发关闭 → OnClose 再去拿同一把锁」的死锁。

---

### D5 idle 检测:每帧重设 read deadline

**决定**:读循环每读一帧之前调 `SetReadDeadline(now + IdleTimeout)`。超时时 `Read` 返回 timeout 错误,读循环退出,原因记为 idle timeout。

**为什么是这个**:不用额外的 goroutine 或 timer。`SetReadDeadline` 不是系统调用,只改 runtime netpoller 里的定时器,每帧调一次的开销可以忽略。超时就是读失败,和其它断线走同一条清理路径。

**为什么不是别的**
- 每条连接一个 `time.Timer`,收到帧就 Reset:能做到同样的事,但多一个要管理生命周期的对象,超时回调在另一个 goroutine 上跑,还要处理和读循环的并发。
- 只用 TCP keepalive:它只能发现「对端机器没了」,发现不了「对端进程还在但卡住了 / 不发数据了」。而且 Linux 默认 2 小时才开始探测。
- 只在连接建立时设一次 deadline:到点就踢,不管中间有没有心跳。

**什么时候会换**:要区分「多久没收到心跳」和「多久没收到业务消息」两种超时(比如挂机检测)时,业务那一层另起 timer。

---

### D5b 关停:Serve 等所有连接的 OnClose 跑完才返回

**决定**:ctx 取消后,Serve 关 listener、关所有连接,然后 `WaitGroup.Wait()` 等每条连接的读写 goroutine 退出,最后才返回。Accept 出错(比如 fd 用尽)时退避重试,不退出。

**为什么是这个**:Lab 2 起 OnClose 里要删 Redis 会话。如果 Serve 不等就返回,main 退出,进程被杀,清理做到一半。Accept 重试和 `net/http.Server` 的做法一致:fd 用尽是暂时的,整个服务退出更糟。

**为什么不是别的**:不等的话关停更快,但 Redis 里会留下一批「幽灵在线」的会话,只能靠 TTL 过期。

**什么时候会换**:连接多到 OnClose 清理要很久时,给等待加上限(比如 10 秒,对应 `docker stop` 的默认宽限期),超时就放弃。

---

## Lab 2 · 登录 + 会话

### D6 token:无状态 HMAC 签名

**决定**:`base64url(playerID | 过期时间) "." base64url(HMAC-SHA256(密钥, 前半段))`。游戏服只要有密钥就能校验,不查任何存储。实现在 `internal/auth`,只用标准库。

**为什么是这个**
- 校验是纯 CPU 运算(一次 HMAC),不依赖 Redis:Redis 挂了,至少「token 对不对」这一步不受影响。
- 签发方(账号服务,开发时是 `cmd/tokengen`)和校验方只共享一个密钥,不共享数据库。
- 三个细节都有测试盯着:先验签名再解析内容(签名没过的字节一个都不信);签名用 `hmac.Equal` 常数时间比较(否则能按响应时间逐字节猜签名);base64 用 `Strict()` 解码(默认解码器会忽略最后一个字符的 2 个填充位,同一个签名能写成好几种字符串。这个坑是测试连跑时撞出来的)。

**为什么不是别的**
- 随机串存 Redis 查表:能随时吊销,但每次登录多一次 Redis 查询,而且 Redis 挂了就完全登录不了。
- JWT:功能上一样,但要引一个库;JWT 的 header 能指定算法,历史上出过 `alg: none` 之类的漏洞。这里只有一种算法,自己写 30 行更容易讲清楚。

**什么时候会换**:要「立刻吊销某个 token」(封号、改密码后踢掉所有设备)时,无状态 token 做不到,要么加一张 Redis 黑名单,要么换成有状态 token。要轮换密钥时,在 token 前面加版本号(`v2.`),新旧密钥并存一段时间。

---

### D7 会话:内存是权威,Redis 是镜像

**决定**:「玩家 → 哪条连接」存在进程内存的 `players` 表里,所有路由(顶号、房间广播)都查它。Redis 里的 `gs:session:{pid}` 是给外部看的在线状态,带 TTL。

**为什么是这个**
- 连接(`*gateway.Conn`)是进程内的对象,本来就放不进 Redis。路由必须查内存。
- Scope 要求在线状态落 Redis:别的进程(以后的账号服务、运维脚本)不连游戏服也能查谁在线。
- 先写 Redis、成功了再改内存:Redis 失败时内存没变,回 `UNAVAILABLE`,客户端可以直接重试,不会出现「内存里登录了、Redis 里没有」的半成品。

**为什么不是别的**
- 只放内存:单进程够用,但 scope 要求的「在线状态落 Redis」就没有了;多实例时也没法查一个玩家在哪个实例上。
- 以 Redis 为权威、每条消息都查 Redis:每条消息多一次网络往返,Redis 一抖整个游戏都卡。

**什么时候会换**:多实例部署、玩家要跨实例找人(私聊、组队)时,Redis 里的 owner 就成了路由表的一部分,要认真处理「两个实例同时认为自己持有某个玩家」的竞争,那时要用分布式锁或者把登录收到一个服务里。

**续期**:一个 goroutine 每 TTL/3 用一次 MULTI/EXEC 覆盖写所有在线玩家(TTL 默认 60 秒,即每 20 秒一次)。没有选「每个 Ping 续一次」:500 人每 5 秒一个 Ping 就是每秒 100 次 Redis 往返,批量续期是每 20 秒 1 次。覆盖写而不是只 EXPIRE,所以 Redis 重启丢了数据,下一轮就补回来(有测试)。

---

### D8 顶号:新登录踢掉旧连接

**决定**:同一账号在第二条连接上登录时,新连接登录成功;旧连接先收到 `Kick{reason: "replaced"}` 再被断开。

**为什么是这个**
- 玩家换设备、断线后重连时,旧连接往往还没被 idle timeout 发现。拒绝新登录的话,玩家要等 30 秒才能进游戏。
- 大多数游戏都是这个行为,玩家有预期。

**为什么不是别的**:拒绝新登录能防「账号被盗后挤掉原主」,但对正常玩家体验差,而且盗号应该在账号层(密码、二次验证)解决。

**什么时候会换**:要做断线重连(选做项)时,新连接不是「踢掉旧的重新开始」,而是「接管旧连接的房间和状态」。

**要能讲清的竞争**:旧连接的 OnClose 和新连接的登录在两个 goroutine 上。清理时必须先确认「`players[pid]` 还是我」,不是就什么都不动;Redis 里也按 owner 删(Lua 脚本里比对再删)。否则旧连接断开时会把新连接的会话删掉。这两个检查都有测试。

---

### D9 Redis key 设计

**决定**:`gs:session:{playerID}` → hash `{owner, login_at}`,TTL 60 秒。`owner` = `{node}#{连接 ID}`,`node` 默认是 `主机名-进程号`。

**为什么是这个**
- hash 而不是 string:以后加字段(在哪个房间、客户端版本)不用改格式。
- `gs:` 前缀:和同一个 Redis 里的其他数据分开,`SCAN gs:*` 能看到本项目的全部 key。
- owner 带进程号:进程重启后连接 ID 从 1 开始重新数,只用连接 ID 会和上一个进程的残留会话撞上。

**为什么不是别的**:用一个大 hash 存全部会话(`gs:sessions` → `{pid: owner}`):查在线人数方便(HLEN),但 hash 里的单个字段不能设 TTL,进程崩溃后残留的会话永远不会过期。

**什么时候会换**:要频繁查「当前在线人数」时,加一个计数器或 sorted set(按最后心跳时间排序),而不是 SCAN。

---

### D10 测试用 miniredis

**决定**:单元测试和集成测试都用 `github.com/alicebob/miniredis/v2`,一个在测试进程里跑的假 Redis。

**为什么是这个**:测试不依赖 Docker,`make test` 在任何机器上都能直接跑;每个测试一个独立实例,互不干扰;能 `FastForward` 时间测 TTL 过期,能 `Close` / `Restart` 测 Redis 宕机,用真 Redis 很难做到。

**为什么不是别的**
- 连真 Redis(没起就 `t.Skip`):最真实,但没装 Docker 的人跑测试时这些测试全部跳过,等于没测。
- 自己写一个内存版的 SessionStore 接口实现:测不到 Lua 脚本和 MULTI/EXEC 本身。

**什么时候会换**:用到 miniredis 不支持的命令(比如某些 Stream、Module 命令)时;Lab 6 有了 docker compose 之后,加一组连真 Redis 的冒烟测试作为补充。

---

### D11 依赖登记

| 依赖 | 用在哪 | 为什么要它 |
|---|---|---|
| `google.golang.org/protobuf` | 编解码 | protobuf 官方 Go 运行时,协议格式的前提 |
| `github.com/redis/go-redis/v9` | `internal/storage` | Redis 官方维护的 Go 客户端,自带连接池、pipeline、Lua 脚本(EVALSHA 失败自动回退 EVAL)。标准库没有 Redis 客户端,自己写 RESP 协议不在 scope 里 |
| `github.com/alicebob/miniredis/v2` | 仅测试 | 见 D10 |

go-redis 另外间接带进来 `xxhash`;miniredis 带进来 `gopher-lua`(执行 Lua 脚本)。
