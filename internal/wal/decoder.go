package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"sync"

	"github.com/datbeohbbh/wal/internal/utils/crc"
	"github.com/datbeohbbh/wal/internal/utils/fileutil"
	"github.com/datbeohbbh/wal/internal/wal/walpb"
)

const (
	minSectorSize = 512
	frameSize     = 8
)

type Decoder interface {
	Decode(*walpb.Record) error
	LastOffset() int64
	LastCRC() uint32
	UpdateCRC(prevCrc uint32)
}

type decoder struct {
	mtx sync.Mutex

	bufReaders []*fileutil.FileBufReader

	lastValidOffset int64
	crc             hash.Hash32

	continueOnCRCError bool
}

func NewDecoderAdvanced(continueOnCRCError bool, r ...fileutil.FileReader) Decoder {
	reader := make([]*fileutil.FileBufReader, len(r))
	for i := range r {
		reader[i] = fileutil.NewFileBufReader(r[i])
	}
	return &decoder{
		bufReaders:         reader,
		lastValidOffset:    0,
		crc:                crc.New(0, crcTable),
		continueOnCRCError: continueOnCRCError,
	}
}

func NewDecoder(r ...fileutil.FileReader) Decoder {
	return NewDecoderAdvanced(false, r...)
}

func (dec *decoder) Decode(record *walpb.Record) error {
	record.Reset()
	dec.mtx.Lock()
	defer dec.mtx.Unlock()
	return dec.decode(record)
}

func (dec *decoder) decode(record *walpb.Record) error {
	if len(dec.bufReaders) == 0 {
		return io.EOF
	}

	reader := dec.bufReaders[0]
	l, err := readInt64(reader)

	if err == io.EOF || (err == nil && l == 0) {
		dec.bufReaders = dec.bufReaders[1:]
		dec.lastValidOffset = 0
		return dec.decode(record)
	}

	if err != nil {
		return err
	}

	lenField, padding := decodeFrameSize(l)
	maxEntryLimit := reader.FileInfo().Size() - dec.lastValidOffset - padding
	if lenField > maxEntryLimit {
		return fmt.Errorf("%w: [wal] max entry size limit exceeded when reading %q, recBytes: %d, fileSize(%d) - offset(%d) - padBytes(%d) = entryLimit(%d)",
			io.ErrUnexpectedEOF,
			reader.FileInfo().Name(),
			lenField,
			reader.FileInfo().Size(),
			dec.lastValidOffset,
			padding, maxEntryLimit)
	}

	data := make([]byte, lenField+padding)
	if _, err = io.ReadFull(reader, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	if err = record.Unmarshal(data[:lenField]); err != nil {
		if dec.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	if record.Type != walpb.Record_CheckSum {
		_, err = dec.crc.Write(record.Data)
		if err != nil {
			return err
		}

		if err = record.Validate(dec.crc.Sum32()); err != nil {
			if !dec.continueOnCRCError {
				record.Reset()
			} else {
				// skip the mismatch entry
				defer func() {
					dec.lastValidOffset += frameSize + lenField + padding
				}()
			}

			if dec.isTornEntry(data) {
				return fmt.Errorf("%w: in file '%s' at position: %d",
					io.ErrUnexpectedEOF,
					reader.FileInfo().Name(),
					dec.lastValidOffset)
			}
			return fmt.Errorf("%w: in file '%s' at position: %d",
				err,
				reader.FileInfo().Name(),
				dec.lastValidOffset)
		}
	}

	dec.lastValidOffset += frameSize + lenField + padding
	/*
		frameSize   +     record size      +   padding  =  wal entry
			(8 byes)				(lenField)           (< 8)
		   +-+         +----------------+      +-+
		   | |         |                |      | |
		   | |         |                |      | |
		   +-+         +----------------+      +-+
	*/
	return nil
}

// isTornEntry determines whether the last entry of the WAL was partially written
// and corrupted because of a torn write.
func (dec *decoder) isTornEntry(data []byte) bool {
	if len(dec.bufReaders) != 1 {
		return false
	}

	isTornChunk := func(d []byte) bool {
		for _, v := range d {
			if v != 0 {
				return false
			}
		}
		return true
	}

	offset := dec.lastValidOffset + frameSize // offset on whole wal file
	cutOff := 0                               // number of entry bytes was read

	for cutOff < len(data) {
		chunkSize := int(minSectorSize - (offset % minSectorSize))
		if chunkSize > len(data)-cutOff {
			chunkSize = len(data) - cutOff
		}

		chunk := data[cutOff : cutOff+chunkSize]
		if isTornChunk(chunk) {
			return true
		}
		offset += int64(chunkSize)
		cutOff += chunkSize
	}

	return false
}

func (dec *decoder) LastOffset() int64 {
	return dec.lastValidOffset
}

func (dec *decoder) LastCRC() uint32 {
	return dec.crc.Sum32()
}

func (dec *decoder) UpdateCRC(prevCrc uint32) {
	dec.crc = crc.New(prevCrc, crcTable)
}

func decodeFrameSize(l int64) (lenField int64, padding int64) {
	lenField = int64(uint64(l) & ^(uint64(0xff) << 56))
	if lenField < 0 {
		padding = int64((uint64(l) >> 56) & 0x7)
	}
	return lenField, padding
}

func readInt64(r io.Reader) (n int64, err error) {
	err = binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
