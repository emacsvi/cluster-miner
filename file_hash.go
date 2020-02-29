package cluster

import (
	"bufio"
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"os"
)

func MD5sum(filename string, piece int) (string, error) {
	return sum(md5.New(), filename, piece)
}

func SHA256sum(filename string, piece int) (string, error) {
	return sum(sha256.New(), filename, piece)
}

func sum(hashAlgorithm hash.Hash, filename string, piece int) (string, error) {
	if info, err := os.Stat(filename); err != nil || info.IsDir() {
		return "", err
	}

	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer func() { _ = file.Close() }()

	return sumReader(hashAlgorithm, bufio.NewReader(file), piece)
}
