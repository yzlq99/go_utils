package cfg

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// MySQLConfiguration  configuration for MySQL database connection
type MySQLConfiguration struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	LogMode  MySQLLogMode
}

// PostgresConfiguration  configuration for Postgres database connection
type PostgresConfiguration struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	LogMode  bool
}

// ESConfiguration  configuration for elasticsearch connection
type ESConfiguration struct {
	Host                         string
	User                         string
	Password                     string
	ResponseHeaderTimeoutSeconds int
}

// RedisConfiguration ...
type RedisConfiguration struct {
	Host string
	Port string
}

// MongoDBConfiguration  configuration for redis connection
type MongoDBConfiguration struct {
	Host   string
	DBName string
}

// InitConfiguration ...
func InitConfiguration(configName string, configPaths []string, config interface{}) error {
	vp := viper.New()
	vp.SetConfigName(configName)
	vp.AutomaticEnv()
	for _, configPath := range configPaths {
		vp.AddConfigPath(configPath)
	}

	if err := vp.ReadInConfig(); err != nil {
		return err
	}

	err := vp.Unmarshal(config)
	if err != nil {
		return err
	}

	return nil
}

// LevelMode ...
type LevelMode string

// LevelMode ...
// warn > info > debug > trace
const (
	LevelWarn  LevelMode = "warn"
	LevelInfo  LevelMode = "info"
	LevelDebug LevelMode = "debug"
	LevelTrace LevelMode = "trace"
)

// Level ...
func (l LevelMode) Level() logrus.Level {
	switch l {
	case LevelWarn:
		return logrus.WarnLevel
	case LevelInfo:
		return logrus.InfoLevel
	case LevelDebug:
		return logrus.DebugLevel
	case LevelTrace:
		return logrus.TraceLevel
	}
	return logrus.WarnLevel
}

// IsDebugMode ...
func (l LevelMode) IsDebugMode() bool {
	return l.Level() >= logrus.DebugLevel
}

// MySQLLogMode ...
type MySQLLogMode string

// Console 使用 gorm 的 logger，打印漂亮的sql到控制台
// SlowQuery 使用自定义 logger.Logger,记录慢查询sql到日志
// None 关闭 log 功能
const (
	Console   MySQLLogMode = "console"
	SlowQuery MySQLLogMode = "slow_query"
	None      MySQLLogMode = "none"
)
