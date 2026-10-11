-- MySQL 建表脚本。docker compose 第一次启动 MySQL(数据卷为空)时由官方镜像自动执行;
-- 集成测试(internal/storage/mysql_test.go)也执行它。全部写成可以重复执行的:
-- CREATE TABLE IF NOT EXISTS、INSERT IGNORE。

-- 账号。登录时 token 里的 player_id 必须在这里能查到。
CREATE TABLE IF NOT EXISTS accounts (
  player_id  BIGINT UNSIGNED NOT NULL PRIMARY KEY,
  name       VARCHAR(64)     NOT NULL,
  created_at DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 对局战绩。一局 = 一个房间从建立到最后一个人离开,解散时写一行。
-- players 是 JSON 数组(按第一次进入的顺序),查某个玩家的战绩用 JSON_CONTAINS / MEMBER OF。
-- (room_id, started_at) 唯一:写入超时重试时,上一次可能已经写进去了,靠它去重。
-- 房间号每次进程重启从 1 开始,所以单靠 room_id 不唯一,加上毫秒级的建房时间才唯一。
CREATE TABLE IF NOT EXISTS match_records (
  id           BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT PRIMARY KEY,
  room_id      INT UNSIGNED     NOT NULL,
  started_at   DATETIME(3)      NOT NULL,
  ended_at     DATETIME(3)      NOT NULL,
  ticks        BIGINT UNSIGNED  NOT NULL,
  peak_players TINYINT UNSIGNED NOT NULL,
  players      JSON             NOT NULL,
  UNIQUE KEY uk_room_started (room_id, started_at),
  KEY idx_ended_at (ended_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 开发和压测用的账号 1–10000(botclient 的玩家 ID 默认从 1 开始连续分配)。
SET SESSION cte_max_recursion_depth = 10000;
INSERT IGNORE INTO accounts (player_id, name)
WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 10000)
SELECT n, CONCAT('player', n) FROM seq;
