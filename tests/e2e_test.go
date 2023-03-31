package tests

import (
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/datbeohbbh/wal"
	w "github.com/datbeohbbh/wal/internal/wal"
	"github.com/datbeohbbh/wal/pb/logpb"
	"github.com/stretchr/testify/require"
)

func TestE2E(t *testing.T) {
	dir := path.Join(t.Name(), "var", "lib", "wal")
	customSegmentSizeBytes(1024 * 1024)

	defer func() {
		walFiles, _ := os.ReadDir(dir)
		for _, w := range walFiles {
			log.Println(w.Name())
		}
		os.RemoveAll(t.Name())

		customSegmentSizeBytes(64 * 1024 * 1024)
	}()

	t.Run("wal", func(t *testing.T) {
		t.Run("create-dir", func(t *testing.T) {
			metadata := []byte("integration tests Write-Ahead Log")
			w, err := wal.Create(dir, metadata)
			require.NoError(t, err)
			require.DirExists(t, dir)

			defer func() {
				_ = w.Close()
			}()
		})

		ents, err := prepareEntryData()
		require.NoError(t, err)

		t.Run("write-entries", func(t *testing.T) {
			w, err := wal.Open(dir)
			require.NoError(t, err)
			require.NotNil(t, w)

			defer func() {
				// sync in close
				_ = w.Close()
			}()

			// write mode so all entries must be read out all before append
			_, _, err = w.ReadAll()
			require.NoError(t, err)

			for _, e := range ents {
				err = w.Save([]logpb.LogEntry{e}, false)
				require.NoError(t, err)
			}
		})

		t.Run("read-entries", func(t *testing.T) {
			w, err := wal.OpenForRead(dir)
			require.NoError(t, err)
			require.NotNil(t, w)

			defer func() {
				_ = w.Close()
			}()

			_, rEnts, err := w.ReadAll()
			require.NoError(t, err)

			require.EqualValues(t, len(ents), len(rEnts))
			for i := range ents {
				require.EqualValues(t, ents[i].Index, rEnts[i].Index)
				require.EqualValues(t, ents[i].Term, rEnts[i].Term)
				require.EqualValues(t, ents[i].CommandName, rEnts[i].CommandName)
				require.EqualValues(t, ents[i].Command, rEnts[i].Command)
			}
		})
	})
}

func newLogEntry(index uint64, term uint64, commandName string, command []byte) logpb.LogEntry {
	return logpb.LogEntry{
		Index:       index,
		Term:        term,
		CommandName: commandName,
		Command:     command,
	}
}

func prepareEntryData() ([]logpb.LogEntry, error) {
	bytesRead, err := os.ReadFile("./data/data.txt")
	if err != nil {
		return nil, err
	}
	data := strings.Split(string(bytesRead), "\n")

	ents := make([]logpb.LogEntry, 0)
	for i, d := range data {
		if d == "" {
			continue
		}
		ents = append(ents,
			newLogEntry(uint64(i),
				1,
				fmt.Sprintf("command#%d", i),
				[]byte(d)))
	}

	return ents, nil
}

func customSegmentSizeBytes(size int64) {
	w.SegmentSizeBytes = size
}
