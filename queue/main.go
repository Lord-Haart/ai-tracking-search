// 该模块实现了消息队列。
// @Author: Haart
// @Created: 2021-10-27
package queue

import (
	"fmt"
	"strconv"

	"github.com/go-redis/redis"
)

var (
	redisHost     string
	redisPort     int
	redisPassword string
	redisDB       int

	redisClient *redis.Client
)

// 初始化Redis队列配置。
// host Redis主机。
// port Redis端口号。
// password Redis口令。
// db Redis队列使用的数据库。
func InitRedisQueue(host string, port int, password string, db int) error {
	redisHost = host
	redisPort = port
	redisPassword = password
	redisDB = db

	client := redis.NewClient(&redis.Options{
		Addr:     redisHost + ":" + strconv.Itoa(redisPort),
		Password: redisPassword,
		DB:       redisDB,
	})
	if client == nil {
		return fmt.Errorf("cannot create redis client")
	}
	if _, err := client.Ping().Result(); err != nil {
		return err
	} else {
		redisClient = client
		return nil
	}
}

// 获取队列的长度。
// topic 主题。
// 返回队列的当前长度。
func Length(topic string) (int64, error) {
	return redisClient.LLen(topic).Result()
}

// 将值入队。
// topic 主题。
// 待入队的值。
// 返回队列的新长度。
func Push(topic string, value string) (int64, error) {
	return redisClient.LPush(topic, value).Result()
}

// 将值出队。
// topic 主题。
// 返回出队的值。
func Pop(topic string) (string, error) {
	return redisClient.RPop(topic).Result()
}
