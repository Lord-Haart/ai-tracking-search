// 该模块实现了消息队列。
// @Author: Haart
// @Created: 2021-10-27
package queue

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-redis/redis/v8"
)

var (
	redisHost     string
	redisPort     int
	redisPassword string
	redisDB       int

	redisClient *redis.Client
	redisCtx    context.Context
)

func init() {
	redisCtx = context.Background()
}

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
	if _, err := client.Ping(redisCtx).Result(); err != nil {
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
	return redisClient.LLen(redisCtx, topic).Result()
}

// 将值入队。
// topic 主题。
// 待入队的值。
// 返回队列的新长度。
func Push(topic string, value string) (int64, error) {
	return redisClient.LPush(redisCtx, topic, value).Result()
}

// 将值出队。
// topic 主题。
// 返回出队的值。
func Pop(topic string) (string, error) {
	return redisClient.RPop(redisCtx, topic).Result()

	// if v, err := redisClient.BRPop(1*time.Second, topic).Result(); err != nil {
	// 	return "", err
	// } else if len(v) < 2 {
	// 	return "", redis.Nil
	// } else {
	// 	return v[1], nil
	// }
}
