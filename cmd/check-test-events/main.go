// Command check-test-events rejects incomplete or skipped qualification runs.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/batchstream/sink-production-suite/internal/testevents"
)

func main() {
	file := flag.String("file", "", "go test -json evidence file")
	require := flag.String("require", "", "comma-separated required test names")
	flag.Parse()
	if err := check(*file, *require); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func check(filename, requirements string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	var required []string
	if requirements != "" {
		required = strings.Split(requirements, ",")
	}
	return testevents.Check(file, required)
}
