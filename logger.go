package gotoqueue

import (
	"log"
	"strings"
	"sync"
)

// Add to logger.go - Enhanced Logger interface with levels
type LogLevel int

const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelError
	LogLevelSilent // No logging
)

const (
	LOG_DEBUG  = "DEBUG"
	LOG_INFO   = "INFO"
	LOG_ERROR  = "ERROR"
	LOG_SILENT = "SILENT"
)

// String representation of log levels
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "DEBUG"
	case LogLevelInfo:
		return "INFO"
	case LogLevelError:
		return "ERROR"
	case LogLevelSilent:
		return "SILENT"
	default:
		return "UNKNOWN"
	}
}

// ParseLogLevel converts string to LogLevel
func ParseLogLevel(level string) LogLevel {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return LogLevelDebug
	case "INFO":
		return LogLevelInfo
	case "ERROR":
		return LogLevelError
	case "SILENT":
		return LogLevelSilent
	default:
		return LogLevelInfo // Default to INFO
	}
}

// Enhanced Logger interface with levels
type Logger interface {
	Debugf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	SetLevel(level LogLevel)
	GetLevel() LogLevel

	// Backward compatibility
	Printf(format string, v ...interface{}) // Maps to Infof
}

// Enhanced DefaultLogger with level support
type DefaultLogger struct {
	level LogLevel
	mutex sync.RWMutex
}

// NewDefaultLogger creates a new default logger with specified level
func NewDefaultLogger(level LogLevel) *DefaultLogger {
	return &DefaultLogger{
		level: level,
	}
}

// SetLevel sets the logging level
func (dl *DefaultLogger) SetLevel(level LogLevel) {
	dl.mutex.Lock()
	defer dl.mutex.Unlock()
	dl.level = level
}

// GetLevel returns the current logging level
func (dl *DefaultLogger) GetLevel() LogLevel {
	dl.mutex.RLock()
	defer dl.mutex.RUnlock()
	return dl.level
}

// shouldLog checks if message should be logged based on level
func (dl *DefaultLogger) shouldLog(messageLevel LogLevel) bool {
	dl.mutex.RLock()
	defer dl.mutex.RUnlock()
	return messageLevel >= dl.level
}

// Debugf logs debug messages
func (dl *DefaultLogger) Debugf(format string, v ...interface{}) {
	if dl.shouldLog(LogLevelDebug) {
		log.Printf("[DEBUG] "+format, v...)
	}
}

// Infof logs info messages
func (dl *DefaultLogger) Infof(format string, v ...interface{}) {
	if dl.shouldLog(LogLevelInfo) {
		log.Printf("[INFO] "+format, v...)
	}
}

// Errorf logs error messages
func (dl *DefaultLogger) Errorf(format string, v ...interface{}) {
	if dl.shouldLog(LogLevelError) {
		log.Printf("[ERROR] "+format, v...)
	}
}

// Printf for backward compatibility - maps to Infof
func (dl *DefaultLogger) Printf(format string, v ...interface{}) {
	dl.Infof(format, v...)
}
