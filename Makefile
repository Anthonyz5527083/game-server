MODULE := github.com/Anthonyz5527083/game-server
BIN    := bin

# 本地开发用的 token 密钥,只为了让 make run / make bot 两边一致。
# 它不是秘密,不能用在任何部署里:部署时用环境变量 GS_TOKEN_SECRET 覆盖。
GS_TOKEN_SECRET ?= local-dev-only-not-a-secret
export GS_TOKEN_SECRET

.PHONY: all build run bot redis test vet fmt proto tidy clean

all: build

## build: 编译 gameserver / botclient / tokengen 到 bin/
build:
	go build -o $(BIN)/gameserver ./cmd/gameserver
	go build -o $(BIN)/botclient  ./cmd/botclient
	go build -o $(BIN)/tokengen   ./cmd/tokengen

## run: 本地启动服务端(默认监听 :7777,连 127.0.0.1:6379 的 Redis)
run: build
	./$(BIN)/gameserver

## bot: 以玩家 1 登录,发 20 个 Ping
bot: build
	./$(BIN)/botclient -player 1

## redis: 用 Docker 起本地开发用的 Redis
redis:
	docker compose -f deploy/docker-compose.yml up -d redis

## test: 带 -race 跑全部测试(Redis 用进程内的 miniredis,不需要 Docker)
test:
	go test -race -count=1 ./...

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
