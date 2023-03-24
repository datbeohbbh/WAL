// `encoder` encodes the data to files.
// data is ensure to be padded before write.
// data will be aligned and written in entire page

package wal

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/datbeohbbh/wal/internal/utils/crc"
	"github.com/datbeohbbh/wal/internal/utils/ioutil"
	"github.com/datbeohbbh/wal/internal/wal/walpb"
)

var (
	walPageSize = (minSectorSize << 3)
	padSize     = 8
)

var (
	poly     = crc32.Castagnoli
	crcTable = crc32.MakeTable(uint32(poly))
)

type encoder struct {
	mtx sync.Mutex

	// Data will be written into page
	bw *ioutil.PageWriter

	crc       hash.Hash32
	buf       []byte
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc32 uint32, pageOffset int) *encoder {
	return &encoder{
		bw:        ioutil.NewPageWriter(w, walPageSize, pageOffset),
		crc:       crc.New(prevCrc32, crcTable),
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

func (enc *encoder) encode(record *walpb.Record) error {
	enc.mtx.Lock()
	defer enc.mtx.Unlock()

	enc.crc.Write(record.Data)
	record.Crc = enc.crc.Sum32()

	var (
		data []byte
		n    int
		err  error
	)

	if record.Size() <= len(enc.buf) {
		n, err = record.MarshalTo(enc.buf)
		if err != nil {
			return err
		}
		data = enc.buf[:n]
	} else {
		data, err = record.Marshal()
		if err != nil {
			return err
		}
	}

	lenField, padding := encodeFrameSize(len(data))
	if err = writeUint64(enc.bw, lenField, enc.uint64buf); err != nil {
		return err
	}

	if padding != 0 {
		data = append(data, make([]byte, padding)...)
	}

	_, err = enc.bw.Write(data)
	return err
}

func encodeFrameSize(l int) (uint64, int) {
	lenField := uint64(l)
	padding := (padSize - (l % padSize)) % padSize
	if padding != 0 {
		lenField |= uint64(0x80|padding) << 56
	}
	return lenField, padding
}

func (enc *encoder) flush() error {
	enc.mtx.Lock()
	defer enc.mtx.Unlock()
	err := enc.bw.Flush()
	return err
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	binary.LittleEndian.PutUint64(buf, n)
	_, err := w.Write(buf)
	return err
}
