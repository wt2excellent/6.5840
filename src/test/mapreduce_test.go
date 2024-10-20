// ------------------------------------------------------------
// package test
// @file      : mapreduce_test.go
// @author    : WeiTao
// @contact   : 15537588047@163.com
// @time      : 2024/10/20 20:37
// ------------------------------------------------------------
package test

import (
	"fmt"
	"strings"
	"testing"
	"unicode"
)

func Test_MapReduce(t *testing.T) {
	contents := "Hello World hi 4"
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)
	fmt.Println(words)
}
