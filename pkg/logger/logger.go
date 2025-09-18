package logger

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
)

func Init(level string) error {
	// 设置日志格式
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: "2006-01-02 15:04:05",
	})

	// 设置输出到标准输出
	logrus.SetOutput(os.Stdout)

	// 解析日志级别
	logLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return fmt.Errorf("invalid log level %s: %w", level, err)
	}

	logrus.SetLevel(logLevel)

	logrus.Infof("Logger initialized with level: %s", level)
	return nil
}