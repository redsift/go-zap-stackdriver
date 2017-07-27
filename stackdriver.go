//
//  stackdriver.go
//  Redsift GCE Stackdriver support for Zap logging
//
//  Created by Rahul Powar on 27/05/2017.
//  Copyright (c) 2015 Redsift Limited. All rights reserved.
//

package stackdriver

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/logging"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type googleEncoder struct {
	lg   *logging.Logger
	buf  map[string]interface{}
	pool buffer.Pool
}

func New(gProjectId, gLogId string) *zap.Logger {
	// setup google stack logging

	ctx := context.Background()

	client, err := logging.NewClient(ctx, gProjectId)
	if err != nil {
		panic(fmt.Sprintf("Failed to create logging client: %v", err))
	}

	lg := client.Logger(gLogId)

	e := NewEncoder(lg)
	w := &googleWriterSyncer{lg}

	core := zapcore.NewCore(
		e,
		w,
		zapcore.DebugLevel,
	)
	return zap.New(core)
}

func NewEncoder(lg *logging.Logger) *googleEncoder {
	buf := make(map[string]interface{})

	return &googleEncoder{lg: lg, buf: buf, pool: buffer.NewPool()}
}

// Key value impl

func (g *googleEncoder) AddArray(key string, marshaler zapcore.ArrayMarshaler) error {
	//TODO: this method
	return nil
}

func (g *googleEncoder) AddObject(key string, marshaler zapcore.ObjectMarshaler) error {
	//TODO: this method
	return nil
}

// Built-in types.
func (g *googleEncoder) AddBinary(key string, value []byte) {
	g.buf[key] = value
}

func (g *googleEncoder) AddByteString(key string, value []byte) {
	g.buf[key] = value
}

func (g *googleEncoder) AddBool(key string, value bool) {
	g.buf[key] = value
}

func (g *googleEncoder) AddComplex128(key string, value complex128) {
	g.buf[key] = value
}

func (g *googleEncoder) AddComplex64(key string, value complex64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddDuration(key string, value time.Duration) {
	g.buf[key] = value
}

func (g *googleEncoder) AddFloat64(key string, value float64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddFloat32(key string, value float32) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt(key string, value int) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt64(key string, value int64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt32(key string, value int32) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt16(key string, value int16) {
	g.buf[key] = value
}

func (g *googleEncoder) AddInt8(key string, value int8) {
	g.buf[key] = value
}

func (g *googleEncoder) AddString(key, value string) {
	g.buf[key] = value
}

func (g *googleEncoder) AddTime(key string, value time.Time) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint(key string, value uint) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint64(key string, value uint64) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint32(key string, value uint32) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint16(key string, value uint16) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUint8(key string, value uint8) {
	g.buf[key] = value
}

func (g *googleEncoder) AddUintptr(key string, value uintptr) {
	g.buf[key] = value
}

// AddReflected uses reflection to serialize arbitrary objects, so it's slow
// and allocation-heavy.
func (g *googleEncoder) AddReflected(key string, value interface{}) error {
	g.buf[key] = value
	return nil
}

func (g *googleEncoder) Clone() zapcore.Encoder {
	buf := make(map[string]interface{})
	for key, value := range g.buf {
		buf[key] = value
	}
	return &googleEncoder{lg: g.lg, buf: buf, pool: g.pool}
}

func (g *googleEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	sev := logging.Default

	switch entry.Level {
	case zapcore.DebugLevel:
		sev = logging.Debug
	case zapcore.InfoLevel:
		sev = logging.Info
	case zapcore.WarnLevel:
		sev = logging.Warning
	case zapcore.ErrorLevel:
		sev = logging.Error
	case zapcore.DPanicLevel:
		sev = logging.Critical
	case zapcore.PanicLevel:
		sev = logging.Alert
	case zapcore.FatalLevel:
		sev = logging.Emergency
	}

	g.buf["msg"] = entry.Message
	e := logging.Entry{Timestamp: entry.Time, Payload: g.buf, Severity: sev}

	g.Free()

	g.lg.Log(e)
	return g.pool.Get(), nil
}

// Encoder impl
func (g *googleEncoder) Free() {
	// no-op
	g.buf = make(map[string]interface{})
}

// OpenNamespace opens an isolated namespace where all subsequent fields will
// be added. Applications can use namespaces to prevent key collisions when
// injecting loggers into sub-components or third-party libraries.
func (g *googleEncoder) OpenNamespace(key string) {
	//TODO
}

/*



*/
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
