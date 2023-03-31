package wal

import (
	internalWAL "github.com/datbeohbbh/wal/internal/wal"
	"github.com/datbeohbbh/wal/pb/logpb"
)

type WAL interface {
	Close() error
	ReadAll() ([]byte, []logpb.LogEntry, error)
	ReleaseLockTo(uint64) error
	Save([]logpb.LogEntry, bool) error
	Sync() error
}

func Create(dirpath string, metadata []byte) (WAL, error) {
	w, err := internalWAL.Create(nil, dirpath, metadata)
	return w, err
}

func Open(dirpath string) (WAL, error) {
	w, err := internalWAL.Open(nil, dirpath)
	return w, err
}

func OpenForRead(dirpath string) (WAL, error) {
	w, err := internalWAL.OpenForRead(nil, dirpath)
	return w, err
}
