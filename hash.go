package cluster

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
)

const bufferSize = 65536

func MD5sumReader(reader io.Reader, piece int) (string, error) {
	return sumReader(md5.New(), reader, piece)
}

func SHA256sumReader(reader io.Reader, piece int) (string, error) {
	return sumReader(sha256.New(), reader, piece)
}

func sumReader(hashAlgorithm hash.Hash, reader io.Reader, piece int) (string, error) {
	buf := make([]byte, bufferSize)
	if piece <= 0 {
		for {
			switch n, err := reader.Read(buf); err {
			case nil:
				hashAlgorithm.Write(buf[:n])
			case io.EOF:
				return fmt.Sprintf("%x", hashAlgorithm.Sum(nil)), nil
			default:
				return "", err
			}
		}
	}

	for i := 0; i <= piece; i++ {
		switch n, err := reader.Read(buf); err {
		case nil:
			hashAlgorithm.Write(buf[:n])
		case io.EOF:
			return fmt.Sprintf("%x", hashAlgorithm.Sum(nil)), nil
		default:
			return "", err
		}
	}

	return fmt.Sprintf("%x", hashAlgorithm.Sum(nil)), nil
}
