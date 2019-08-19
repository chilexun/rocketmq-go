package mqclient

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type LoggerLevel int8

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in
	// production.
	Debug = LoggerLevel(zapcore.DebugLevel)
	// InfoLevel is the default logging priority.
	Info = LoggerLevel(zapcore.InfoLevel)
	// WarnLevel logs are more important than Infof, but don't need individual
	// human review.
	Warn = LoggerLevel(zapcore.WarnLevel)
	// ErrorLevel logs are high-priority. If an application is running smoothly,
	// it shouldn't generate any error-level logs.
	Error = LoggerLevel(zapcore.ErrorLevel)
	// DPanicLevel logs are particularly important errors. In development the
	// logger panics after writing the message.
	DPanic = LoggerLevel(zapcore.DPanicLevel)
	// PanicLevel logs a message, then panics.
	Panic = LoggerLevel(zapcore.PanicLevel)
	// FatalLevel logs a message, then calls os.Exit(1).
	Fatal = LoggerLevel(zapcore.FatalLevel)
)

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

	zapLoggerConfig        = zap.NewDevelopmentConfig()
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

func SetLogger(customLogger Logger) {
	logger = customLogger
}

func GetLogger() Logger {
	return logger
}

// SetLoggerLevel
func SetLoggerLevel(level LoggerLevel) error {
	zapLoggerConfig.Level = zap.NewAtomicLevelAt(zapcore.Level(level))
	zapLogger, err := zapLoggerConfig.Build()
	if err != nil {
		return err
	}
	logger = zapLogger.Sugar()
	return nil
}
