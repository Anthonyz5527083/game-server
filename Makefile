MODULE := github.com/Anthonyz5527083/game-server
BIN    := bin

# 本地开发用的 token 密钥,只为了让 make run / make bot 两边一致。
# 它不是秘密,不能用在任何部署里:部署时用环境变量 GS_TOKEN_SECRET 覆盖。
GS_TOKEN_SECRET ?= local-dev-only-not-a-secret
export GS_TOKEN_SECRET

COMPOSE := docker compose -f deploy/docker-compose.yml

.PHONY: all build run bot watch bench redis up down down-v logs test test-mysql vet fmt proto tidy clean

all: build

## build: 编译 gameserver / botclient / tokengen 到 bin/
build:
	go build -o $(BIN)/gameserver ./cmd/gameserver
	go build -o $(BIN)/botclient  ./cmd/botclient
	go build -o $(BIN)/tokengen   ./cmd/tokengen

## run: 本地启动服务端(默认监听 :7777,连 127.0.0.1:6379 的 Redis;admin 在 127.0.0.1:9100)
## 要查账号表、记战绩就设 GS_MYSQL_DSN,例如 GS_MYSQL_DSN='game:local-dev-only@tcp(127.0.0.1:3306)/game'
run: build
	./$(BIN)/gameserver

## bot: 以玩家 1 登录,发 20 个 Ping
bot: build
	./$(BIN)/botclient -player 1

## watch: 以玩家 1 进房间并绕圈走,打印快照。另开终端跑 ./bin/botclient -player 2 -watch 看它动
watch: build
	./$(BIN)/botclient -player 1 -watch -move

## bench: 500 连接压测(预热 5 秒 + 测量 30 秒),负载模型见 docs/BENCHMARK.md
bench: build
	./$(BIN)/botclient -c 500 -d 30s -warmup 5s

## redis: 只起 Redis(本地 go run 服务端时用)
redis:
	$(COMPOSE) up -d redis

## up: docker compose 起全套(gameserver + Redis + MySQL),会先构建镜像
up:
	$(COMPOSE) up -d --build

## down: 停掉全套;down-v 连 MySQL 数据卷一起删(下次 up 重新建表)
down:
	$(COMPOSE) down

down-v:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f gameserver

## test: 带 -race 跑全部测试(Redis 用进程内的 miniredis,不需要 Docker;MySQL 的集成测试会跳过)
test:
	go test -race -count=1 ./...

## test-mysql: 连一个真 MySQL 跑存储层的集成测试。库会被清空,别指向有用的数据
GS_TEST_MYSQL_DSN ?= game:local-dev-only@tcp(127.0.0.1:3306)/game
test-mysql:
	GS_TEST_MYSQL_DSN='$(GS_TEST_MYSQL_DSN)' go test -race -count=1 -run 'Accounts|InsertMatch' -v ./internal/storage

vet:
	go vet ./...

fmt:
	gofmt -l -w .

## proto: 由 proto/*.proto 生成 internal/pb/*.pb.go(生成物入库,clone 下来就能 build)
proto:
	protoc -I proto --go_out=. --go_opt=module=$(MODULE) proto/*.proto

tidy:
	go mod tidy

clean:
	rm -rf $(BIN)
