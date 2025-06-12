package gotoqueue

import "log"

type Logger interface {
	Printf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
}

type DefaultLogger struct{}

func (dl *DefaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

func (dl *DefaultLogger) Errorf(format string, v ...interface{}) {
	log.Printf("ERROR: "+format, v...)
}
