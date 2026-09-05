MODULE := github.com/Anthonyz5527083/game-server
BIN    := bin

.PHONY: all build run bot test vet fmt proto tidy clean

all: build

## build: 编译 gameserver 与 botclient 到 bin/
build:
	go build -o $(BIN)/gameserver ./cmd/gameserver
	go build -o $(BIN)/botclient  ./cmd/botclient

## run: 本地启动服务端(默认监听 :7777)
run: build
	./$(BIN)/gameserver

## bot: 启动 bot client 对本地服务端发 ping
bot: build
	./$(BIN)/botclient

## test: 带 -race 跑全部测试
test:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

## proto: 由 proto/*.proto 生成 internal/pb/*.pb.go(生成物入库,clone 即可 build)
proto:
	protoc -I proto --go_out=. --go_opt=module=$(MODULE) proto/*.proto

tidy:
	go mod tidy

clean:
	rm -rf $(BIN)
