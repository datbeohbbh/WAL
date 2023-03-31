package walpb

import (
	"errors"
	fmt "fmt"
)

var (
	ErrCRCMismatch = errors.New("walpb: crc mismatch")
)

func (record *Record) Validate(crc uint32) error {
	if record.Crc == crc {
		return nil
	}
	return fmt.Errorf("%w: expected: %x computed: %x", ErrCRCMismatch, record.Crc, crc)
}
