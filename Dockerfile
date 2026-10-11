# syntax=docker/dockerfile:1
# 多阶段构建:build 阶段用完整的 Go 镜像编译,运行阶段只放静态二进制(D25)。
#   docker build -t game-server .        或者    make up(用 deploy/docker-compose.yml 起全套)

FROM golang:1.27-alpine AS build
WORKDIR /src
# 先只拷 go.mod / go.sum 下载依赖:改了源码、没改依赖时,这一层直接用缓存,不用重新下载。
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# CGO_ENABLED=0:纯静态链接,运行镜像里不需要 libc。-trimpath 去掉构建机的路径;
# -s -w 去掉 ELF 符号表和 DWARF。pprof 不受影响:Go 的 profile 用的是 runtime 自带的函数表(pclntab)。
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/gameserver ./cmd/gameserver \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/botclient ./cmd/botclient \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/tokengen ./cmd/tokengen

# distroless/static:只有 CA 证书、时区数据和 /etc/passwd,没有 shell 和包管理器。
# botclient / tokengen 也放进来,容器里可以直接 exec 它们做冒烟测试。
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
EXPOSE 7777 9100
# exec 形式:gameserver 是 PID 1,直接收到 docker stop 的 SIGTERM,走优雅关停(D5b)。
ENTRYPOINT ["/usr/local/bin/gameserver"]
