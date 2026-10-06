// Package storage 封装 Redis(在线会话 / 房间镜像)访问。Part B 的 MySQL(账号 / 战绩)也放这里。
//
// 约定:这一层只负责「怎么存」,不做业务判断。所有 key 都带 gs: 前缀,
// 和同一个 Redis 里的其它数据分开。
package storage
