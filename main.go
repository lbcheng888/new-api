package main

import (
	"embed"
	// "flag" // 不再需要 flag 包
	"fmt"
	"log"
	"net/http"
	"one-api/common"
	"one-api/constant"
	"one-api/controller"
	"one-api/middleware"
	"one-api/model"
	"one-api/router"
	"one-api/service"
	// "one-api/setting/operation_setting" // Removed as it's no longer used directly
	"os"
	"strconv"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-contrib/sessions"
	"github.com/gin-contrib/sessions/cookie"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"

	_ "net/http/pprof"
)

//go:embed web/dist
var buildFS embed.FS

//go:embed web/dist/index.html
var indexPage []byte

func main() {
	// 确定要加载的配置文件路径
	envFile := ".env" // 默认尝试加载 .env 文件
	// 检查是否有命令行参数提供
	if len(os.Args) > 1 {
		// 第一个参数 os.Args[0] 是程序本身的名字
		// 所以第一个用户提供的参数是 os.Args[1]
		envFile = os.Args[1]
		common.SysLog(fmt.Sprintf("Loading configuration from command line argument: %s", envFile))
	} else {
		common.SysLog(fmt.Sprintf("No configuration file specified via command line argument, attempting to load default: %s", envFile))
	}

	// 尝试加载配置文件
	err := godotenv.Load(envFile)
	if err != nil {
		// 如果加载失败，记录日志，但程序继续执行，依赖于系统环境变量或默认值
		common.SysLog(fmt.Sprintf("Warning: Failed to load configuration file '%s': %s. Proceeding with environment variables and defaults.", envFile, err.Error()))
	} else {
		common.SysLog(fmt.Sprintf("Successfully loaded configuration from %s", envFile))
	}

	// 加载系统环境变量，这将覆盖 .env 文件中定义的任何同名变量
	common.LoadEnv()
	common.SetupLogger()
	common.SysLog("New API " + common.Version + " started")
	if os.Getenv("GIN_MODE") != "debug" {
		gin.SetMode(gin.ReleaseMode)
	}
	if common.DebugEnabled {
		common.SysLog("running in debug mode")
	}
	// Initialize SQL Database
	err = model.InitDB()
	if err != nil {
		common.FatalLog("failed to initialize database: " + err.Error())
	}

	model.CheckSetup()

	// Initialize SQL Database
	err = model.InitLogDB()
	if err != nil {
		common.FatalLog("failed to initialize database: " + err.Error())
	}
	defer func() {
		err := model.CloseDB()
		if err != nil {
			common.FatalLog("failed to close database: " + err.Error())
		}
	}()

	// Initialize Redis
	err = common.InitRedisClient()
	if err != nil {
		common.FatalLog("failed to initialize Redis: " + err.Error())
	}

<<<<<<< HEAD
	// Initialize model settings
	operation_setting.InitRatioSettings()
=======
	// Model settings (ratios, prices) are initialized lazily on first access.
	// No explicit initialization needed here anymore.
>>>>>>> 6ab83975 (update main.go)
	// Initialize constants
	constant.InitEnv()
	// Initialize options
	model.InitOptionMap()

	if common.RedisEnabled {
		// for compatibility with old versions
		common.MemoryCacheEnabled = true
	}
	if common.MemoryCacheEnabled {
		common.SysLog("memory cache enabled")
		common.SysError(fmt.Sprintf("sync frequency: %d seconds", common.SyncFrequency))
		model.InitChannelCache()
	}
	if common.MemoryCacheEnabled {
		go model.SyncOptions(common.SyncFrequency)
		go model.SyncChannelCache(common.SyncFrequency)
	}

	// 数据看板
	go model.UpdateQuotaData()

	if os.Getenv("CHANNEL_UPDATE_FREQUENCY") != "" {
		frequency, err := strconv.Atoi(os.Getenv("CHANNEL_UPDATE_FREQUENCY"))
		if err != nil {
			common.FatalLog("failed to parse CHANNEL_UPDATE_FREQUENCY: " + err.Error())
		}
		go controller.AutomaticallyUpdateChannels(frequency)
	}
	// if os.Getenv("CHANNEL_TEST_FREQUENCY") != "" {
	// 	frequency, err := strconv.Atoi(os.Getenv("CHANNEL_TEST_FREQUENCY"))
	// 	if err != nil {
	// 		common.FatalLog("failed to parse CHANNEL_TEST_FREQUENCY: " + err.Error())
	// 	}
	// 	go controller.AutomaticallyTestChannels(frequency)
	// }
	if common.IsMasterNode && constant.UpdateTask {
		gopool.Go(func() {
			controller.UpdateMidjourneyTaskBulk()
		})
		gopool.Go(func() {
			controller.UpdateTaskBulk()
		})
	}
	if os.Getenv("BATCH_UPDATE_ENABLED") == "true" {
		common.BatchUpdateEnabled = true
		common.SysLog("batch update enabled with interval " + strconv.Itoa(common.BatchUpdateInterval) + "s")
		model.InitBatchUpdater()
	}

	if os.Getenv("ENABLE_PPROF") == "true" {
		gopool.Go(func() {
			log.Println(http.ListenAndServe("0.0.0.0:8005", nil))
		})
		go common.Monitor()
		common.SysLog("pprof enabled")
	}

	service.InitTokenEncoders()

	// Initialize HTTP server
	server := gin.New()
	server.Use(gin.CustomRecovery(func(c *gin.Context, err any) {
		common.SysError(fmt.Sprintf("panic detected: %v", err))
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": gin.H{
				"message": fmt.Sprintf("Panic detected, error: %v. Please submit a issue here: https://github.com/Calcium-Ion/new-api", err),
				"type":    "new_api_panic",
			},
		})
	}))
	// This will cause SSE not to work!!!
	//server.Use(gzip.Gzip(gzip.DefaultCompression))
	server.Use(middleware.RequestId())
	middleware.SetUpLogger(server)
	// Initialize session store
	store := cookie.NewStore([]byte(common.SessionSecret))
	store.Options(sessions.Options{
		Path:     "/",
		MaxAge:   2592000, // 30 days
		HttpOnly: true,
		Secure:   false,
		SameSite: http.SameSiteStrictMode,
	})
	server.Use(sessions.Sessions("session", store))

	router.SetRouter(server, buildFS, indexPage)
	var port = os.Getenv("PORT")
	if port == "" {
		port = strconv.Itoa(*common.Port)
	}
	err = server.Run(":" + port)
	if err != nil {
		common.FatalLog("failed to start HTTP server: " + err.Error())
	}
}
