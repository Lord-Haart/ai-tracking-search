package utils

import (
	"crypto/md5"
	"encoding/hex"
	"strings"
)

func SignWithMd5(args ...string) string {
	plainText := strings.Join(args, "")
	md5Bytes := md5.Sum([]byte(plainText))
	return hex.EncodeToString(md5Bytes[:])
}

func VerifyWithMd5(sign string, args ...string) bool {
	plainText := strings.Join(args, "")
	md5Bytes := md5.Sum([]byte(plainText))
	return hex.EncodeToString(md5Bytes[:]) == sign
}
