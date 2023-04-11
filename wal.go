package wal

import (
	internalWAL "github.com/datbeohbbh/wal/internal/wal"
	"github.com/datbeohbbh/wal/pb/logpb"
	"go.uber.org/zap"
)

type WAL interface {
	Close() error
	ReadAll() ([]byte, []logpb.LogEntry, error)
	ReleaseLockTo(uint64) error
	Save([]logpb.LogEntry, bool) error
	Sync() error
}

func Create(lg *zap.Logger, dirpath string, metadata []byte) (WAL, error) {
	w, err := internalWAL.Create(lg, dirpath, metadata)
	return w, err
}

func Open(lg *zap.Logger, dirpath string) (WAL, error) {
	w, err := internalWAL.Open(lg, dirpath)
	return w, err
}

func OpenForRead(dirpath string) (WAL, error) {
	w, err := internalWAL.OpenForRead(nil, dirpath)
	return w, err
}
