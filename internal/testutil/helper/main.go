// Command helper is a tiny deterministic child process used by workgate's
// integration tests: it optionally appends start/end markers to a shared
// file, sleeps, and exits with a chosen code. It is not part of the
// distributed tool.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"
)

func main() {
	sleep := flag.Duration("sleep", 0, "how long to sleep between markers")
	exitCode := flag.Int("exit", 0, "exit code")
	marker := flag.String("marker", "", "file to append start/end markers to")
	name := flag.String("name", "helper", "marker name")
	flag.Parse()

	appendMarker(*marker, "start "+*name)
	if *sleep > 0 {
		time.Sleep(*sleep)
	}
	appendMarker(*marker, "end "+*name)
	os.Exit(*exitCode)
}

func appendMarker(path, line string) {
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "helper: ", err)
		return
	}
	defer f.Close()
	fmt.Fprintln(f, line)
}
