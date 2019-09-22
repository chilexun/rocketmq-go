package mqclient

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

//LogLevel specifies the log levels
type LogLevel int8

const (
	// Debug Level logs are typically voluminous, and are usually disabled in
	// production.
	Debug = LogLevel(zapcore.DebugLevel)
	// Info Level is the default logging priority.
	Info = LogLevel(zapcore.InfoLevel)
	// Warn Level logs are more important than Infof, but don't need individual
	// human review.
	Warn = LogLevel(zapcore.WarnLevel)
	// Error Level logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	Error = LogLevel(zapcore.ErrorLevel)
	// DPanic Level logs are particularly important errors. In development the
	// logger panics after writing the message.
	DPanic = LogLevel(zapcore.DPanicLevel)
	// Panic Level logs a message, then panics.
	Panic = LogLevel(zapcore.PanicLevel)
	// Fatal Level logs a message, then calls os.Exit(1).
	Fatal = LogLevel(zapcore.FatalLevel)
)

//Logger write the log info with specify level
type Logger interface {
	Info(args ...interface{})
	Warn(args ...interface{})
	Error(args ...interface{})
	Debug(args ...interface{})

	Infof(fmt string, args ...interface{})
	Warnf(fmt string, args ...interface{})
	Errorf(fmt string, args ...interface{})
	Debugf(fmt string, args ...interface{})
}

var (
	logger Logger

	zapLoggerConfig        = zap.NewProductionConfig()
	zapLoggerEncoderConfig = zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		EncodeLevel:    zapcore.CapitalColorLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
)

func init() {
	zapLoggerConfig.DisableCaller = true
	zapLoggerConfig.EncoderConfig = zapLoggerEncoderConfig
	zapLogger, _ := zapLoggerConfig.Build()
	logger = zapLogger.Sugar()
}

//SetLogger replace the logger with self define impl
func SetLogger(customLogger Logger) {
	logger = customLogger
}

//GetLogger returns the logger current used
func GetLogger() Logger {
	return logger
}

// SetLogLevel change the log level and realtime effective
func SetLogLevel(level LogLevel) error {
	zapLoggerConfig.Level = zap.NewAtomicLevelAt(zapcore.Level(level))
	zapLogger, err := zapLoggerConfig.Build()
	if err != nil {
		return err
	}
	logger = zapLogger.Sugar()
	return nil
}
