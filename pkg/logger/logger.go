package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
)

// ColoredFormatter is a custom formatter with color support
type ColoredFormatter struct {
	*logrus.TextFormatter
}

// Format formats the log entry with colors and better structure
func (f *ColoredFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	// Color codes for different log levels
	var levelColor string
	switch entry.Level {
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		levelColor = "\033[31m" // Red
	case logrus.WarnLevel:
		levelColor = "\033[33m" // Yellow
	case logrus.InfoLevel:
		levelColor = "\033[36m" // Cyan
	case logrus.DebugLevel:
		levelColor = "\033[35m" // Magenta
	default:
		levelColor = "\033[37m" // White
	}

	// Reset color
	const resetColor = "\033[0m"
	
	// Format timestamp
	timestamp := entry.Time.Format("2006-01-02 15:04:05.000")
	
	// Format level with padding and color
	level := strings.ToUpper(entry.Level.String())
	paddedLevel := fmt.Sprintf("%-5s", level)
	
	// Build the message with colors
	var message strings.Builder
	message.WriteString(fmt.Sprintf("\033[90m%s%s", timestamp, resetColor)) // Gray timestamp
	message.WriteString(fmt.Sprintf(" %s[%s]%s", levelColor, paddedLevel, resetColor))
	
	// Add caller info if available
	if entry.HasCaller() {
		caller := fmt.Sprintf("%s:%d", filepath.Base(entry.Caller.File), entry.Caller.Line)
		message.WriteString(fmt.Sprintf(" \033[90m[%s]%s", caller, resetColor))
	}
	
	message.WriteString(fmt.Sprintf(" %s", entry.Message))
	
	// Add fields if any
	if len(entry.Data) > 0 {
		message.WriteString(" \033[90m{")
		first := true
		for k, v := range entry.Data {
			if !first {
				message.WriteString(", ")
			}
			message.WriteString(fmt.Sprintf("%s=%v", k, v))
			first = false
		}
		message.WriteString(fmt.Sprintf("}%s", resetColor))
	}
	
	message.WriteString("\n")
	return []byte(message.String()), nil
}

// Config holds logger configuration
type Config struct {
	Level      string
	EnableFile bool
	FilePath   string
	EnableCaller bool
}

// DefaultConfig returns default logger configuration
func DefaultConfig() *Config {
	return &Config{
		Level:        "info",
		EnableFile:   false,
		FilePath:     "logs/app.log",
		EnableCaller: false,
	}
}

// Init initializes the logger with enhanced features
func Init(level string) error {
	return InitWithConfig(&Config{
		Level:        level,
		EnableFile:   false,
		EnableCaller: false,
	})
}

// InitWithConfig initializes the logger with custom configuration
func InitWithConfig(cfg *Config) error {
	// 设置自定义格式化器
	logrus.SetFormatter(&ColoredFormatter{
		TextFormatter: &logrus.TextFormatter{
			FullTimestamp:   true,
			TimestampFormat: "2006-01-02 15:04:05.000",
			ForceColors:     true,
		},
	})

	// 设置是否显示调用信息
	logrus.SetReportCaller(cfg.EnableCaller)

	// 设置输出
	if cfg.EnableFile {
		if err := setupFileOutput(cfg.FilePath); err != nil {
			return fmt.Errorf("failed to setup file output: %w", err)
		}
	} else {
		logrus.SetOutput(os.Stdout)
	}

	// 解析日志级别
	logLevel, err := logrus.ParseLevel(cfg.Level)
	if err != nil {
		return fmt.Errorf("invalid log level %s: %w", cfg.Level, err)
	}

	logrus.SetLevel(logLevel)

	// 使用结构化日志记录初始化信息
	WithFields(logrus.Fields{
		"level":       cfg.Level,
		"file_output": cfg.EnableFile,
		"caller":      cfg.EnableCaller,
	}).Info("Logger initialized successfully")
	
	return nil
}

// setupFileOutput configures file output for logging
func setupFileOutput(filePath string) error {
	// 创建日志目录
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create log directory: %w", err)
	}

	// 打开日志文件
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	// 同时输出到文件和控制台
	multiWriter := io.MultiWriter(os.Stdout, file)
	logrus.SetOutput(multiWriter)

	return nil
}

// WithFields creates a new entry with the given fields
func WithFields(fields logrus.Fields) *logrus.Entry {
	return logrus.WithFields(fields)
}

// WithField creates a new entry with a single field
func WithField(key string, value interface{}) *logrus.Entry {
	return logrus.WithField(key, value)
}

// Info logs an info message
func Info(args ...interface{}) {
	logrus.Info(args...)
}

// Infof logs a formatted info message
func Infof(format string, args ...interface{}) {
	logrus.Infof(format, args...)
}

// Warn logs a warning message
func Warn(args ...interface{}) {
	logrus.Warn(args...)
}

// Warnf logs a formatted warning message
func Warnf(format string, args ...interface{}) {
	logrus.Warnf(format, args...)
}

// Error logs an error message
func Error(args ...interface{}) {
	logrus.Error(args...)
}

// Errorf logs a formatted error message
func Errorf(format string, args ...interface{}) {
	logrus.Errorf(format, args...)
}

// Debug logs a debug message
func Debug(args ...interface{}) {
	logrus.Debug(args...)
}

// Debugf logs a formatted debug message
func Debugf(format string, args ...interface{}) {
	logrus.Debugf(format, args...)
}

// Fatal logs a fatal message and exits
func Fatal(args ...interface{}) {
	logrus.Fatal(args...)
}

// Fatalf logs a formatted fatal message and exits
func Fatalf(format string, args ...interface{}) {
	logrus.Fatalf(format, args...)
}