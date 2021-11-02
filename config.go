// 该模块定义了配置类型。
// @Author: Haart
// @Created: 2021-10-27
package main

// Configuration 表示全局配置对象。
type Configuration struct {
	Listen  string // 提供服务的绑定地址。
	Timeout int    // 读取和写入的超时（秒）

	DB DBConfiguration // 数据库设置。

	Redis RedisConfiguration // Redis配置。
}

type DBConfiguration struct {
	DSN string // 连接数据库的字符串。
}

type RedisConfiguration struct {
	Host     string // Redis 的地址。
	Port     int    // Redis 的端口。
	Password string // Redis 的口令。
	DB       int    // 使用的Redis数据库。
}
