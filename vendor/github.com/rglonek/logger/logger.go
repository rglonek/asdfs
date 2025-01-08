package logger

import (
	"fmt"
	"log"
	"os"
)

var defaultLevel = 4

var defaultPrefix = ""

type Logger struct {
	logLevel      int
	p             string
	disableStderr bool
	logToFile     string
	enableKmesg   bool
	fileLogger    *log.Logger
	kmesg         *os.File
}

func (l *Logger) SinkDisableStderr() {
	l.disableStderr = true
}

func (l *Logger) SinkLogToFile(name string) (err error) {
	l.logToFile = name
	f, err := os.OpenFile(name, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	l.fileLogger = log.New(f, "", log.Default().Flags())
	return nil
}

func (l *Logger) SinkEnableKmesg() error {
	l.enableKmesg = true
	kmsg, err := os.OpenFile("/dev/kmsg", os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	l.kmesg = kmsg
	return nil
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
	format = l.p + "INFO " + format
	if !l.disableStderr {
		log.Printf(format, v...)
	}
	if l.fileLogger != nil {
		l.fileLogger.Printf(format, v...)
	}
	if l.kmesg != nil {
		fmt.Fprintf(l.kmesg, "<5>"+format+"\n", v...)
	}
}

func (l *Logger) Warn(format string, v ...interface{}) {
	if l.logLevel < 3 {
		return
	}
	format = l.p + "WARNING " + format
	if !l.disableStderr {
		log.Printf(format, v...)
	}
	if l.fileLogger != nil {
		l.fileLogger.Printf(format, v...)
	}
	if l.kmesg != nil {
		fmt.Fprintf(l.kmesg, "<4>"+format+"\n", v...)
	}
}

func (l *Logger) Error(format string, v ...interface{}) {
	if l.logLevel < 2 {
		return
	}
	format = l.p + "ERROR " + format
	if !l.disableStderr {
		log.Printf(format, v...)
	}
	if l.fileLogger != nil {
		l.fileLogger.Printf(format, v...)
	}
	if l.kmesg != nil {
		fmt.Fprintf(l.kmesg, "<3>"+format+"\n", v...)
	}
}

func (l *Logger) Critical(format string, v ...interface{}) {
	if l.logLevel >= 1 {
		format = l.p + "CRITICAL " + format
		if !l.disableStderr {
			log.Printf(format, v...)
		}
		if l.fileLogger != nil {
			l.fileLogger.Printf(format, v...)
		}
		if l.kmesg != nil {
			fmt.Fprintf(l.kmesg, "<2>"+format+"\n", v...)
		}
	}
	os.Exit(1)
}

func (l *Logger) Debug(format string, v ...interface{}) {
	if l.logLevel < 5 {
		return
	}
	format = l.p + "DEBUG " + format
	if !l.disableStderr {
		log.Printf(format, v...)
	}
	if l.fileLogger != nil {
		l.fileLogger.Printf(format, v...)
	}
	if l.kmesg != nil {
		fmt.Fprintf(l.kmesg, "<6>"+format+"\n", v...)
	}
}

func (l *Logger) Detail(format string, v ...interface{}) {
	if l.logLevel < 6 {
		return
	}
	format = l.p + "DETAIL " + format
	if !l.disableStderr {
		log.Printf(format, v...)
	}
	if l.fileLogger != nil {
		l.fileLogger.Printf(format, v...)
	}
	if l.kmesg != nil {
		fmt.Fprintf(l.kmesg, "<7>"+format+"\n", v...)
	}
}
