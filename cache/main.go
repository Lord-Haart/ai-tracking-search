// 该模块实现了缓存。
// @Author: Haart
// @Created: 2021-10-27
package cache

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var (
	redisHost     string
	redisPort     int
	redisPassword string
	redisDB       int
	redisClient   *redis.Client
	redisCtx      context.Context
)

func init() {
	redisCtx = context.Background()
}

// 初始化Redis队列配置。
// host Redis主机。
// port Redis端口号。
// password Redis口令。
// db Redis队列使用的数据库。
func InitRedisCache(host string, port int, password string, db int) error {
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

// Deprecated 此方法会清除之前设置的过期时间。
// func Set(key string, fields map[string]interface{}) error {
// 	_, err := redisClient.HMSet(key, fields).Result()
// 	return err
// }

// 保存指定的值到缓存，并设置过期时间。
// key 缓存的键。
// fields 缓存的内容。
// expiration 缓存过期的时间。
func SetAndExpire(key string, fields map[string]interface{}, expiration time.Duration) error {
	p := redisClient.TxPipeline()

	p.HMSet(redisCtx, key, fields)
	p.Expire(redisCtx, key, expiration)

	if _, err := p.Exec(redisCtx); err != nil {
		return err
	} else {
		return nil
	}
}

func Update(key string, fields map[string]interface{}) error {
	p := redisClient.Pipeline()

	for hk, hv := range fields {
		p.HSet(redisCtx, key, hk, hv)
	}

	if _, err := p.Exec(redisCtx); err != nil {
		return err
	} else {
		return nil
	}
}

// 获取缓存内容。
// key 缓存的键。
// fields 缓存内容的名字。
// 返回被缓存的内容。
func Get(key string, fields ...string) ([]interface{}, error) {
	if r, err := redisClient.HMGet(redisCtx, key, fields...).Result(); err != nil {
		return nil, err
	} else {
		allNil := true
		for _, o := range r {
			if o != nil {
				allNil = false
			}
		}
		if allNil {
			return nil, redis.Nil
		} else {
			return r, nil
		}
	}
}

// 删除缓存。
// key 缓存的键。
func Del(key string) (int64, error) {
	return redisClient.Del(redisCtx, key).Result()
}

// 获取并删除缓存内容。
// key 缓存的键。
// fields 缓存内容的名字。
// 返回被缓存的内容。
func Take(key string, fields ...string) ([]interface{}, error) {
	p := redisClient.Pipeline()

	p.HMGet(redisCtx, key, fields...)
	p.Del(redisCtx, key)

	if cc, err := p.Exec(redisCtx); err != nil {
		return nil, err
	} else {
		return cc[0].(*redis.SliceCmd).Result()
	}
}

// 获取缓存内容并延长过期时间。
// key 缓存的键。
// expiration 缓存延长的过期时间。
// fields 缓存内容的名字。
// 返回被缓存的内容。
func GetAndExpire(key string, expiration time.Duration, fields ...string) ([]interface{}, error) {
	p := redisClient.Pipeline()

	p.HMGet(redisCtx, key, fields...).Result()
	p.Expire(redisCtx, key, expiration)

	if cc, err := p.Exec(redisCtx); err != nil {
		return nil, err
	} else {
		return cc[0].(*redis.SliceCmd).Result()
	}
}
