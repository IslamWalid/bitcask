package bitcask

import (
	"os"
	"path"
	"testing"
)

var testBitcaskPath = path.Join("testing_dir")

func TestOpen(t *testing.T) {
	t.Run("open new bitcask with read and write permission", func(t *testing.T) {
		Open(testBitcaskPath, ReadWrite)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
			t.Errorf("Expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open new bitcask with sync_on_put option", func(t *testing.T) {
		Open(testBitcaskPath, ReadWrite, SyncOnPut)

		if _, err := os.Stat(testBitcaskPath); os.IsNotExist(err) {
			t.Errorf("Expected to find directory: %q", testBitcaskPath)
		}
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open new bitcask with default options", func(t *testing.T) {
		_, err := Open(testBitcaskPath)
		assertError(t, err, "open testing_dir: no such file or directory")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open existing bitcask with write permission", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Put("key12", "value12345")
		b1.Close()

		b2, _ := Open(testBitcaskPath, ReadWrite)

		want := "value12345"
		got, _ := b2.Get("key12")
		b2.Close()

		assertString(t, got, want)
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("two readers in the same bitcask at the same time", func(t *testing.T) {
		b1, _ := Open(testBitcaskPath, ReadWrite)
		b1.Put("key2", "value2")
		b1.Put("key3", "value3")
		b1.Close()

		b2, _ := Open(testBitcaskPath)
		b3, _ := Open(testBitcaskPath)

		want := "value2"
		got, _ := b2.Get("key2")
		assertString(t, got, want)
		b2.Close()

		got, _ = b3.Get("key2")
		assertString(t, got, want)
		b3.Close()
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open existing bitcask with hint files in it", func(t *testing.T) {
		// implemented after implementing merge
	})

	t.Run("open bitcask with writer exists in it", func(t *testing.T) {
		Open(testBitcaskPath, ReadWrite)
		_, err := Open(testBitcaskPath)

		assertError(t, err, "access denied: datastore is locked")
		os.RemoveAll(testBitcaskPath)
	})

	t.Run("open bitcask failed", func(t *testing.T) {
		// create a directory that cannot be openned since it has no execute permission
		os.MkdirAll(path.Join("no open dir"), 000)

		want := "open no open dir: permission denied"
		_, err := Open("no open dir")

		assertError(t, err, want)
		os.RemoveAll("no open dir")
	})
}

func assertError(t testing.TB, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected Error %q", want)
	}
	assertString(t, err.Error(), want)
}

func assertString(t testing.TB, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("got:\n%q\nwant:\n%q", got, want)
	}
}
