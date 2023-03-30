package wal

import internalWAL "github.com/datbeohbbh/wal/internal/wal"

// Exist returns true if there are any `.wal` files in a given directory.
func ExistWALDir(dirpath string) bool {
	return internalWAL.Exist(dirpath)
}
