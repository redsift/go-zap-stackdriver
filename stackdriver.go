//
//  stackdriver.go
//  Redsift GCE Stackdriver support for Zap logging
//
//  Created by Rahul Powar on 27/05/2017.
//  Copyright (c) 2015 Redsift Limited. All rights reserved.
//

package stackdriver

import (
	"cloud.google.com/go/logging"
	"github.com/uber-go/zap"
	"io"
	"time"
)

type googleEncoder struct {
	lg  *logging.Logger
	buf map[string]interface{}
}

func NewEncoder(lg *logging.Logger) *googleEncoder {
	buf := make(map[string]interface{})
	return &googleEncoder{lg: lg, buf: buf}
}

// Key value impl

func (g *googleEncoder) AddBool(key string, value bool) {
	g.buf[key] = value
}

func (g *googleEncoder) AddFloat64(key string, value float64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt(key string, value int) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt64(key string, value int64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint(key string, value uint) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint64(key string, value uint64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUintptr(key string, value uintptr) {
	g.buf[key] = value
}

func (g *googleEncoder) AddMarshaler(key string, marshaler zap.LogMarshaler) error {
	//TODO: this method
	return nil
}

// AddObject uses reflection to serialize arbitrary objects, so it's slow and
// allocation-heavy. Consider implementing the LogMarshaler interface instead.
func (g *googleEncoder) AddObject(key string, value interface{}) error {
	g.buf[key] = value
	return nil
}

func (g *googleEncoder) AddString(key, value string) {
	g.buf[key] = value
}

// Encoder impl
func (g *googleEncoder) Free() {
	// no-op
	g.buf = make(map[string]interface{})
}

func (g *googleEncoder) Clone() zap.Encoder {
	buf := make(map[string]interface{})
	for key, value := range g.buf {
		buf[key] = value
	}
	return &googleEncoder{lg: g.lg, buf: buf}
}

func (g *googleEncoder) WriteEntry(_ io.Writer, msg string, level zap.Level, time time.Time) error {
	sev := logging.Default

	switch level {
	case zap.DebugLevel:
		sev = logging.Debug
	case zap.InfoLevel:
		sev = logging.Info
	case zap.WarnLevel:
		sev = logging.Warning
	case zap.ErrorLevel:
		sev = logging.Error
	case zap.DPanicLevel:
		sev = logging.Critical
	case zap.PanicLevel:
		sev = logging.Alert
	case zap.FatalLevel:
		sev = logging.Emergency
	}

	g.buf["msg"] = msg
	e := logging.Entry{Timestamp: time, Payload: g.buf, Severity: sev}
	g.lg.Log(e)
	return nil
}

type googleWriterSyncer struct {
	lg *logging.Logger
}

func (g *googleWriterSyncer) Write(b []byte) (int, error) {
	// devnull, the encoder does the work
	return len(b), nil
}

func (g *googleWriterSyncer) Sync() error {
	// but it does want sync events
	g.lg.Flush()

	return nil
}