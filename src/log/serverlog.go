package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"os"
)

var SaveDBLogger *zap.SugaredLogger
var allLogger map[string]*zap.SugaredLogger

func GetLogger(name string) *zap.SugaredLogger {
	v, ok := allLogger[name]
	if !ok {
		return nil
	}
	return v
}

type LogConfig struct {
	Path string `json:"path"`
	// 日志大小限制，单位MB
	MaxSize int `json:"maxSize"`
	// 历史日志文件保留天数
	MaxAge int `json:"maxAge"`
	// 最大保留历史日志数量
	MaxBackups   int               `json:"maxBackups"`
	DefaultLevel string            `json:"defaultLevel"`
	Levels       map[string]string `json:"levels"`
}

func createErrorLoger(v *LogConfig) zapcore.Core {
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.ConsoleSeparator = "\t"
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	encoderConfig.EncodeDuration = zapcore.SecondsDurationEncoder
	encoderConfig.EncodeName = zapcore.FullNameEncoder
	encoderConfig.FunctionKey = "func"
	// 日志轮转
	writer := &lumberjack.Logger{
		// 日志名称
		Filename: v.Path + "/error.log",
		// 日志大小限制，单位MB
		MaxSize: v.MaxSize,
		// 历史日志文件保留天数
		MaxAge: v.MaxAge,
		// 最大保留历史日志数量
		MaxBackups: v.MaxBackups,
		// 本地时区
		LocalTime: true,
		// 历史日志文件压缩标识
		Compress: false,
	}
	zapCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(writer),
		zap.ErrorLevel,
	)
	return zapCore
}
func createLogger(config *LogConfig, name string, logLevel string, errorCore zapcore.Core) *zap.SugaredLogger {
	if logLevel == "" {
		logLevel = config.DefaultLevel
	}
	atomicLevel := zap.NewAtomicLevel()
	switch logLevel {
	case "DEBUG":
		atomicLevel.SetLevel(zapcore.DebugLevel)
	case "INFO":
		atomicLevel.SetLevel(zapcore.InfoLevel)
	case "WARN":
		atomicLevel.SetLevel(zapcore.WarnLevel)
	case "ERROR":
		atomicLevel.SetLevel(zapcore.ErrorLevel)
	case "DPANIC":
		atomicLevel.SetLevel(zapcore.DPanicLevel)
	case "PANIC":
		atomicLevel.SetLevel(zapcore.PanicLevel)
	case "FATAL":
		atomicLevel.SetLevel(zapcore.FatalLevel)
	}
	//encoderConfig := zapcore.EncoderConfig{
	//	TimeKey:        "time",
	//	LevelKey:       "level",
	//	NameKey:        "name",
	//	CallerKey:      "line",
	//	MessageKey:     "msg",
	//	FunctionKey:    "func",
	//	StacktraceKey:  "stacktrace",
	//	LineEnding:     zapcore.DefaultLineEnding,
	//	EncodeLevel:    zapcore.LowercaseLevelEncoder,
	//	EncodeTime:     zapcore.ISO8601TimeEncoder,
	//	EncodeDuration: zapcore.SecondsDurationEncoder,
	//	EncodeCaller:   zapcore.FullCallerEncoder,
	//	EncodeName:     zapcore.FullNameEncoder,
	//}
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.ConsoleSeparator = "\t"
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoderConfig.EncodeCaller = zapcore.ShortCallerEncoder
	encoderConfig.EncodeDuration = zapcore.SecondsDurationEncoder
	encoderConfig.EncodeName = zapcore.FullNameEncoder
	encoderConfig.FunctionKey = "func"
	// 日志轮转
	writer := &lumberjack.Logger{
		// 日志名称
		Filename: config.Path + "/" + name + ".log",
		// 日志大小限制，单位MB
		MaxSize: config.MaxSize,
		// 历史日志文件保留天数
		MaxAge: config.MaxAge,
		// 最大保留历史日志数量
		MaxBackups: config.MaxBackups,
		// 本地时区
		LocalTime: true,
		// 历史日志文件压缩标识
		Compress: false,
	}
	zapCore := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(writer),
		atomicLevel,
	)
	zapCoreCon := zapcore.NewCore(
		zapcore.NewConsoleEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		atomicLevel,
	)
	zaptree := zapcore.NewTee(
		zapCore, zapCoreCon, errorCore)

	logger := zap.New(zaptree, zap.AddCaller(), zap.AddStacktrace(zap.FatalLevel))

	defer logger.Sync()
	return logger.Sugar()
}

func InitLog(config *LogConfig) {
	if config.MaxSize == 0 {
		config.MaxSize = 100
	}
	if config.MaxAge == 0 {
		config.MaxAge = 5
	}
	if config.MaxBackups == 0 {
		config.MaxAge = 100
	}
	allLogger = map[string]*zap.SugaredLogger{}
	errorCore := createErrorLoger(config)
	SaveDBLogger = createLogger(config, "saveDB", config.Levels["saveDB"], errorCore)
	allLogger["saveDB"] = SaveDBLogger
	for k, v := range config.Levels {
		if k != "saveDB" {
			allLogger[k] = createLogger(config, k, v, errorCore)
		}
	}
}
