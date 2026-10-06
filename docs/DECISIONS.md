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
