package wal

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/datbeohbbh/wal/internal/utils/fileutil"
	"github.com/datbeohbbh/wal/internal/wal/logpb"
	"github.com/datbeohbbh/wal/internal/wal/walpb"
)

func TestNew(t *testing.T) {
	p := t.TempDir()

	w, err := Create(zaptest.NewLogger(t), p, []byte("somedata"))
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if g := filepath.Base(w.tail().Name()); g != walName(0, 0) {
		t.Errorf("name = %+v, want %+v", g, walName(0, 0))
	}
	defer w.Close()

	err = w.Sync()
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	// file is preallocated to segment size; only read data written by wal
	off, err := w.tail().Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatal(err)
	}
	gd := make([]byte, off)
	f, err := os.Open(filepath.Join(p, filepath.Base(w.tail().Name())))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = io.ReadFull(f, gd); err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	var wb bytes.Buffer
	e := newEncoder(&wb, 0, 0)
	err = e.encode(&walpb.Record{Type: CrcType, Crc: 0})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	err = e.encode(&walpb.Record{Type: MetadataType, Data: []byte("somedata")})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}

	e.flush()
	if !bytes.Equal(gd, wb.Bytes()) {
		t.Errorf("data = %v, want %v", gd, wb.Bytes())
	}
}

func TestCreateFailFromPollutedDir(t *testing.T) {
	p := t.TempDir()
	os.WriteFile(filepath.Join(p, "test.wal"), []byte("data"), os.ModeTemporary)

	_, err := Create(zaptest.NewLogger(t), p, []byte("data"))
	if err != os.ErrExist {
		t.Fatalf("expected %v, got %v", os.ErrExist, err)
	}
}

func TestWalCleanup(t *testing.T) {
	testRoot := t.TempDir()
	p, err := os.MkdirTemp(testRoot, "waltest")
	if err != nil {
		t.Fatal(err)
	}

	logger := zaptest.NewLogger(t)
	w, err := Create(logger, p, []byte(""))
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	w.cleanupWAL(logger)
	fnames, err := fileutil.ReadDir(testRoot)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(fnames) != 1 {
		t.Fatalf("expected 1 file under %v, got %v", testRoot, len(fnames))
	}
	pattern := fmt.Sprintf(`%s.broken\.[\d]{8}\.[\d]{6}\.[\d]{1,6}?`, filepath.Base(p))
	match, _ := regexp.MatchString(pattern, fnames[0])
	if !match {
		t.Errorf("match = false, expected true for %v with pattern %v", fnames[0], pattern)
	}
}

func TestCreateFailFromNoSpaceLeft(t *testing.T) {
	p := t.TempDir()

	oldSegmentSizeBytes := SegmentSizeBytes
	defer func() {
		SegmentSizeBytes = oldSegmentSizeBytes
	}()
	SegmentSizeBytes = math.MaxInt64

	_, err := Create(zaptest.NewLogger(t), p, []byte("data"))
	if err == nil { // no space left on device
		t.Fatalf("expected error 'no space left on device', got nil")
	}
}

func TestNewForInitedDir(t *testing.T) {
	p := t.TempDir()

	os.Create(filepath.Join(p, walName(0, 0)))
	if _, err := Create(zaptest.NewLogger(t), p, nil); err == nil || err != os.ErrExist {
		t.Errorf("err = %v, want %v", err, os.ErrExist)
	}
}

func TestSaveWithCut(t *testing.T) {
	p := t.TempDir()

	w, err := Create(zaptest.NewLogger(t), p, []byte("metadata"))
	if err != nil {
		t.Fatal(err)
	}

	bigData := make([]byte, 500)
	strdata := "Hello World!!"
	copy(bigData, strdata)
	// set a lower value for SegmentSizeBytes, else the test takes too long to complete
	restoreLater := SegmentSizeBytes
	const EntrySize int = 500
	SegmentSizeBytes = 2 * 1024
	defer func() { SegmentSizeBytes = restoreLater }()
	index := uint64(0)
	for totalSize := 0; totalSize < int(SegmentSizeBytes); totalSize += EntrySize {
		ents := []logpb.LogEntry{{Index: index, Term: 1, Command: bigData}}
		if err = w.Save(ents, true); err != nil {
			t.Fatal(err)
		}
		index++
	}

	w.Close()

	neww, err := Open(zaptest.NewLogger(t), p)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	defer neww.Close()
	wname := walName(1, index)
	if g := filepath.Base(neww.tail().Name()); g != wname {
		t.Errorf("name = %s, want %s", g, wname)
	}

	meta, entries, err := neww.ReadAll()
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal([]byte("metadata"), meta) {
		t.Errorf("metadata does not match: found: %s , want :%s", meta, []byte("metadata"))
	}

	if len(entries)-1 != int(SegmentSizeBytes/int64(EntrySize)) { // do not include metadata entry
		t.Errorf("Number of entries = %d, expected = %d", len(entries), int(SegmentSizeBytes/int64(EntrySize)))
	}
	for _, oneent := range entries {
		if !bytes.Equal(oneent.Command, bigData) {
			t.Errorf("the saved data does not match at Index %d : found: %s , want :%s", oneent.Index, oneent.Command, bigData)
		}
	}
}
