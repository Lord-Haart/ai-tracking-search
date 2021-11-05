// 该模块是入口模块。
// @Author: Haart
// @Created: 2021-10-27
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"

	_cache "com.cne/ai-tracking-search/cache"
	_queue "com.cne/ai-tracking-search/queue"
	"github.com/gin-gonic/gin"

	_agent "com.cne/ai-tracking-search/agent"
	_db "com.cne/ai-tracking-search/db"
	_rpc "com.cne/ai-tracking-search/rpc"
)

const (
	AppName    string = "tracking-search" // 表示应用程序名。
	AppVersion string = "0.1.0"           // 表示应用程序版本。

	DefaultConfigFile    string = "./" + AppName + ".json" // 表示默认的配置文件名。
	DefaultListenAddress string = ":8001"                  // 表示默认的监听地址。
	DefaultDebug         bool   = false                    // 表示默认是否开启Debug模式。
	DefaultTimeout       int    = 30                       // 表示默认的请求超时秒数。

	DefaultRedisHost     string = "localhost" // 表示默认的Redis主机地址。
	DefaultRedisPort     int    = 6379        // 表示默认的Redis端口号。
	DefaultRedisPassword string = ""          // 表示默认的Redis口令。
	DefaultRedisDB       int    = 1           // 表示默认的Redis数据库。

	DefaultAgentPollingBatchSize int = 50 // 表示默认的轮询批量数。
)

var (
	flagVersion bool // 是否显示版权信息
	flagHelp    bool // 是否显示帮助信息
	flagVerify  bool // 是否只检查配置文件
	flagDebug   bool // 是否显示调试信息

	configuration *Configuration = &Configuration{
		Listen:  DefaultListenAddress,
		Timeout: DefaultTimeout,
		Redis: RedisConfiguration{
			Host:     DefaultRedisHost,
			Port:     DefaultRedisPort,
			Password: DefaultRedisPassword,
			DB:       DefaultRedisDB,
		},
		Agent: AgentConfiguration{
			PollingBatchSize: DefaultAgentPollingBatchSize,
		},
	}
)

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	flag.BoolVar(&flagVersion, "version", false, "Shows version message")
	flag.BoolVar(&flagHelp, "h", false, "Shows this help message")
	flag.BoolVar(&flagVerify, "verify", false, "Verify configuration and quit")
	flag.BoolVar(&flagDebug, "debug", DefaultDebug, "Show debugging information")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s -version\n", AppName)
		fmt.Fprintf(os.Stderr, "Usage: %s -h\n", AppName)
		fmt.Fprintf(os.Stderr, "Usage: %s -verify\n", AppName)
		fmt.Fprintf(os.Stderr, "Usage: %s [-debug] [CONFIG_FILE]\n", AppName)
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	if flagVersion {
		fmt.Printf("%s version %s\n", AppName, AppVersion)
		return
	}

	if flagHelp {
		flag.Usage()
		return
	}

	if !flagDebug {
		log.SetOutput(ioutil.Discard)
	}

	if flagDebug {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// 加载配置。
	if err := loadConfig(strings.TrimSpace(flag.Arg(0))); err != nil {
		panic(fmt.Errorf("cannot load configuration: %w", err))
	}

	if flagVerify {
		fmt.Printf("configuration:\n%#v\n", configuration)
		return
	}

	// 输出pid文件。
	pidFilename := AppName + "-pid"
	if runtime.GOOS != "windows" {
		pidFilename = "/var/run/" + pidFilename
	}
	if pidFile, err := os.Create(pidFilename); err == nil {
		pidFile.WriteString(fmt.Sprintf("%v", os.Getpid()))
		pidFile.Close()

		defer os.Remove(pidFilename)
	}

	// 初始化数据库。
	if err := _db.InitDB(configuration.DB.DSN); err != nil {
		panic(err)
	}

	// 初始化Redis缓存。
	if err := _cache.InitRedisCache(configuration.Redis.Host, configuration.Redis.Port, configuration.Redis.Password, configuration.Redis.DB); err != nil {
		panic(err)
	}

	// 初始化Redis队列。
	if err := _queue.InitRedisQueue(configuration.Redis.Host, configuration.Redis.Port, configuration.Redis.Password, configuration.Redis.DB); err != nil {
		panic(err)
	}

	// 初始化轮询参数。
	if err := _agent.InitAgent(configuration.Agent.PollingBatchSize); err != nil {
		panic(err)
	}

	// 开始服务。
	err := serveForEver()
	if err != nil {
		panic(err)
	}

}

func loadConfig(configFile string) (err error) {
	if configFile == "" {
		configFile = DefaultConfigFile
	}

	configFile, err = filepath.Abs(configFile)
	if err != nil {
		return err
	}

	configFileStat, err := os.Stat(configFile)
	if err != nil {
		return err
	}

	if configFileStat.IsDir() {
		configFile = filepath.Join(configFile, DefaultConfigFile)
	}

	err = loadConfigFromFile(configFile)
	if err != nil {
		return err
	}

	// 检查服务绑定地址的格式是否正确。
	configuration.Listen = strings.ToLower(strings.TrimSpace(configuration.Listen))
	if configuration.Listen == "" || configuration.Listen == ":" {
		configuration.Listen = DefaultListenAddress
	} else if !strings.HasPrefix(configuration.Listen, ":") {
		return fmt.Errorf("listen address should start with colon(:), do you prefer %v ?", ":"+configuration.Listen)
	}

	// 检查数据库DSN的格式是否正确。
	configuration.DB.DSN = strings.TrimSpace(configuration.DB.DSN)
	if configuration.DB.DSN == "" || !strings.Contains(configuration.DB.DSN, "@") || !strings.Contains(configuration.DB.DSN, ":") {
		return fmt.Errorf("dsn should contains at(@) and colon(:)")
	}

	return err
}

func loadConfigFromFile(configFile string) (err error) {
	var cf *os.File

	fmt.Printf("Loading configuration from %s ...\n", configFile)

	if cf, err = os.Open(configFile); err != nil {
		return err
	}

	defer cf.Close()

	dec := json.NewDecoder(cf)
	err = dec.Decode(&configuration)
	if err != nil {
		return err
	}

	return nil
}

func serveForEver() error {
	go doServe()
	go _agent.PollForEver()

	// 启动守护routine。
	sigChannel := make(chan os.Signal, 256)
	signal.Notify(sigChannel, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM)
	for {
		sig := <-sigChannel
		fmt.Fprintf(os.Stderr, "Received sig: %#v\n", sig)
		switch sig {
		case syscall.SIGINT, syscall.SIGQUIT, syscall.SIGTERM:
			return nil
		}
	}
}

func doServe() error {
	router := gin.Default()

	// 路由表
	router.POST("/carriers", _rpc.Carriers)
	router.POST("/trackings", _rpc.Trackings)

	fmt.Printf("Serving @ %s\n", configuration.Listen)

	return router.Run(configuration.Listen)
}
