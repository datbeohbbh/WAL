package ioutil

import (
	"fmt"
	"io"
)

var defaultBufferSize = 128 * 1024

type PageWriter struct {
	w io.Writer

	// Page size
	pageSize int

	// Page offset
	pageOffset int

	// The number of bytes which was written to buffer.
	bufferedBytes int

	buf []byte

	bufWatermark int
}

func NewPageWriter(w io.Writer, pageSize, pageOffset int) *PageWriter {
	if pageSize <= 0 {
		panic(fmt.Sprintf("invalid pageBytes (%d) value, it must be greater than 0", pageSize))
	}
	return &PageWriter{
		w:             w,
		pageSize:      pageSize,
		pageOffset:    pageOffset,
		bufferedBytes: 0,
		buf:           make([]byte, defaultBufferSize+pageSize),
		bufWatermark:  defaultBufferSize,
	}
}

func (pw *PageWriter) Write(data []byte) (n int, err error) {
	if pw.bufferedBytes+len(data) <= pw.bufWatermark {
		n = copy(pw.buf[pw.bufferedBytes:], data)
		pw.bufferedBytes += len(data)
		return n, nil
	}

	fit := (pw.pageOffset+pw.bufferedBytes)%pw.pageSize == 0
	if !fit {
		slack := pw.pageSize - ((pw.pageOffset + pw.bufferedBytes) % pw.pageSize)
		// fill the additional page
		partial := slack > len(data)
		if partial {
			slack = len(data)
		}
		n = copy(pw.buf[pw.bufferedBytes:], data[:slack])
		pw.bufferedBytes += slack
		data = data[slack:]
		if partial {
			return n, nil
		}
	}

	// in case non-partial: all page now aligned => flush buffer
	if err = pw.Flush(); err != nil {
		return n, err
	}

	if len(data) >= pw.pageSize {
		pages := len(data) / pw.pageSize
		c, err := pw.w.Write(data[:pages*pw.pageSize])
		n += c
		if err != nil {
			return n, err
		}
		data = data[pages*pw.pageSize:]
	}

	c, err := pw.Write(data)
	n += c
	return n, err
}

func (pw *PageWriter) Flush() error {
	if _, err := pw.flush(); err != nil {
		return err
	}
	return nil
}

func (pw *PageWriter) flush() (n int, err error) {
	if pw.bufferedBytes == 0 {
		return 0, nil
	}
	n, err = pw.w.Write(pw.buf[:pw.bufferedBytes])
	// Reset offset to offset of current page.
	// Reset buffer.
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageSize
	pw.bufferedBytes = 0
	return n, err
}
