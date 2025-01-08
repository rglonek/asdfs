package main

import (
	"fmt"
	"os"

	"github.com/rglonek/logger"
)

var log = logger.NewLogger()

func main() {
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s /path/to/config.yaml dest/\n", os.Args[0])
		os.Exit(1)
	}
	log.Info("Mounting from %s to %s", os.Args[1], os.Args[2])
	c, err := NewConfigFromFile(os.Args[1])
	if err != nil {
		log.Critical("%s", err)
	}
	c.MountDir = os.Args[2]
	log.SetLogLevel(c.LogLevel)
	log.Info("Connecting to aerospike")
	asd, err := Connect(c)
	if err != nil {
		log.Critical("%s", err)
	}
	log.Info("Executing Mount")
	err = Mount(c, asd)
	if err != nil {
		log.Critical("%s", err)
	}
}
