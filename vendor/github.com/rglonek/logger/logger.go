package logger

import (
	"log"
	"os"
)

var defaultLevel = 4

var defaultPrefix = ""

type Logger struct {
	logLevel int
	p        string
}

var defaultLogger = &Logger{
	logLevel: 4, // 0=NO_LOGGING 1=CRITICAL, 2=ERROR, 3=WARNING, 4=INFO, 5=DEBUG, 6=DETAIL
	p:        "",
}

func Info(format string, v ...interface{}) {
	defaultLogger.Info(format, v...)
}

func Warn(format string, v ...interface{}) {
	defaultLogger.Warn(format, v...)
}

func Error(format string, v ...interface{}) {
	defaultLogger.Error(format, v...)
}

func Critical(format string, v ...interface{}) {
	defaultLogger.Critical(format, v...)
}

func Debug(format string, v ...interface{}) {
	defaultLogger.Debug(format, v...)
}

func Detail(format string, v ...interface{}) {
	defaultLogger.Detail(format, v...)
}

func SetPrefix(prefix string) {
	defaultLogger.SetPrefix(prefix)
	defaultPrefix = prefix
}

func SetLogLevel(level int) {
	defaultLogger.SetLogLevel(level)
	defaultLevel = level
}

func NewLogger() *Logger {
	return &Logger{
		logLevel: defaultLevel,
		p:        defaultPrefix,
	}
}

func (l *Logger) SetPrefix(prefix string) {
	l.p = prefix
}

func (l *Logger) SetLogLevel(level int) {
	if level < 0 {
		level = 0
	}
	l.logLevel = level
}

func (l *Logger) Info(format string, v ...interface{}) {
	if l.logLevel < 4 {
		return
	}
	format = "INFO " + l.p + format
	log.Printf(format, v...)
}

func (l *Logger) Warn(format string, v ...interface{}) {
	if l.logLevel < 3 {
		return
	}
	format = "WARNING " + l.p + format
	log.Printf(format, v...)
}

func (l *Logger) Error(format string, v ...interface{}) {
	if l.logLevel < 2 {
		return
	}
	format = "ERROR " + l.p + format
	log.Printf(format, v...)
}

func (l *Logger) Critical(format string, v ...interface{}) {
	if l.logLevel >= 1 {
		format = "CRITICAL " + l.p + format
		log.Printf(format, v...)
	}
	os.Exit(1)
}

func (l *Logger) Debug(format string, v ...interface{}) {
	if l.logLevel < 5 {
		return
	}
	format = "DEBUG " + l.p + format
	log.Printf(format, v...)
}

func (l *Logger) Detail(format string, v ...interface{}) {
	if l.logLevel < 6 {
		return
	}
	format = "DETAIL " + l.p + format
	log.Printf(format, v...)
}
