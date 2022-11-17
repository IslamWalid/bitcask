package main

import (
	"flag"
	"fmt"
	"os"

	resp "github.com/IslamWalid/bitcask/pkg/respserver"
)

func main() {
	pathPtr := flag.String("d", "", "specify the desired datastore path")
	port := flag.Int("p", 6379, "specify the desired server port")
	flag.Parse()

	s, err := resp.New(*pathPtr, fmt.Sprintf(":%d", *port))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer s.Close()

	err = s.ListenAndServe()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
