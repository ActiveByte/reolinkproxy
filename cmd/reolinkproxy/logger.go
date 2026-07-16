package main

import (
	"fmt"
	stdlog "log"
	"os"
	"strings"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// logRingBuffer is a fixed-capacity, thread-safe tail of recent log lines, so the web UI can
// show a live log view without needing filesystem/docker-logs access - the main way of
// checking "did something go wrong" on a headless deployment like unraid.
type logRingBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

func newLogRingBuffer(capacity int) *logRingBuffer {
	return &logRingBuffer{cap: capacity, lines: make([]string, 0, capacity)}
}

// Write implements io.Writer/zapcore.WriteSyncer, splitting on newlines so each log record
// becomes one buffered entry regardless of how the caller batches writes.
func (r *logRingBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, line := range strings.Split(string(p), "\n") {
		if line == "" {
			continue
		}
		r.lines = append(r.lines, line)
	}
	if over := len(r.lines) - r.cap; over > 0 {
		r.lines = r.lines[over:]
	}
	return len(p), nil
}

func (r *logRingBuffer) Sync() error { return nil }

// Lines returns up to the last n buffered lines (all of them if n <= 0), oldest first.
func (r *logRingBuffer) Lines(n int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if n <= 0 || n > len(r.lines) {
		n = len(r.lines)
	}
	out := make([]string, n)
	copy(out, r.lines[len(r.lines)-n:])
	return out
}

type appLogger struct {
	mu     sync.RWMutex
	level  zap.AtomicLevel
	base   *zap.Logger
	sugar  *zap.SugaredLogger
	buffer *logRingBuffer
}

var log = newAppLogger()

func newAppLogger() *appLogger {
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	buffer := newLogRingBuffer(2000)
	base := buildZapLogger(level, buffer)
	return &appLogger{
		level:  level,
		base:   base,
		sugar:  base.Sugar(),
		buffer: buffer,
	}
}

func buildZapLogger(level zap.AtomicLevel, buffer *logRingBuffer) *zap.Logger {
	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "time"
	encoderCfg.LevelKey = "level"
	encoderCfg.MessageKey = "msg"
	encoderCfg.CallerKey = ""
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderCfg.EncodeLevel = zapcore.LowercaseLevelEncoder
	encoder := zapcore.NewConsoleEncoder(encoderCfg)

	// Every log record goes to both stderr (docker logs / journal / terminal) and the
	// in-memory ring buffer the web UI reads from.
	writer := zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stderr), buffer)
	core := zapcore.NewCore(encoder, writer, level)

	return zap.New(core, zap.AddCallerSkip(1))
}

func (l *appLogger) Configure(rawLevel string) error {
	level, err := parseLogLevel(rawLevel)
	if err != nil {
		return err
	}
	l.level.SetLevel(level)
	stdlog.SetFlags(0)
	stdlog.SetOutput(l.buffer)
	return nil
}

// SetLevel changes the log level at runtime (e.g. from the web UI), without a restart.
func (l *appLogger) SetLevel(rawLevel string) error {
	level, err := parseLogLevel(rawLevel)
	if err != nil {
		return err
	}
	l.level.SetLevel(level)
	return nil
}

// Level returns the current log level name.
func (l *appLogger) Level() string {
	return l.level.Level().String()
}

// RecentLines returns up to the last n buffered log lines (all of them if n <= 0).
func (l *appLogger) RecentLines(n int) []string {
	return l.buffer.Lines(n)
}

func parseLogLevel(raw string) (zapcore.Level, error) {
	if strings.TrimSpace(raw) == "" {
		return zap.InfoLevel, nil
	}

	var level zapcore.Level
	if err := level.Set(strings.ToLower(strings.TrimSpace(raw))); err != nil {
		return zap.InfoLevel, fmt.Errorf("invalid log level %q", raw)
	}
	return level, nil
}

func (l *appLogger) Sync() {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.base != nil {
		_ = l.base.Sync()
	}
}

func (l *appLogger) Debugf(format string, args ...any) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.sugar.Debugf(format, args...)
}

func (l *appLogger) Infof(format string, args ...any) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.sugar.Infof(format, args...)
}

func (l *appLogger) Warnf(format string, args ...any) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.sugar.Warnf(format, args...)
}

func (l *appLogger) Errorf(format string, args ...any) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.sugar.Errorf(format, args...)
}

func (l *appLogger) Fatalf(format string, args ...any) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	l.sugar.Fatalf(format, args...)
}

func (l *appLogger) Printf(format string, args ...any) {
	l.Infof(format, args...)
}

func (l *appLogger) Print(args ...any) {
	l.Infof("%s", fmt.Sprint(args...))
}

func (l *appLogger) Println(args ...any) {
	l.Infof("%s", fmt.Sprintln(args...))
}

func (l *appLogger) Fatal(args ...any) {
	l.Fatalf("%s", fmt.Sprint(args...))
}
