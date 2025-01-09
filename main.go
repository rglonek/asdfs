package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/rglonek/logger"
)

var log = logger.NewLogger()

func main() {
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s /path/to/config.yaml dest/\n", os.Args[0])
		os.Exit(1)
	}
	c, err := NewConfigFromFile(os.Args[1])
	if err != nil {
		log.Critical("%s", err)
	}
	c.MountDir = os.Args[2]

	log.Info("%v", os.Args)
	if len(os.Args) >= 5 {
		if os.Args[3] != "-o" {
			log.Critical("Invalid argument (%v)", os.Args)
		}
		for _, param := range strings.Split(strings.ToLower(os.Args[3]), ",") {
			switch param {
			case "rw":
				c.MountParams.RW = true
				c.MountParams.RO = false
			case "ro":
				c.MountParams.RW = false
				c.MountParams.RO = true
			}
		}
	}

	log.SetLogLevel(c.Log.Level)
	log.SetPrefix("asdfs: ")
	if !c.Log.Stderr {
		log.SinkDisableStderr()
	}
	if c.Log.File != "" {
		err = log.SinkLogToFile(c.Log.File)
		if err != nil {
			log.Critical("Create File Log Sink: %s", err)
		}
	}
	if c.Log.Kmesg {
		err = log.SinkEnableKmesg()
		if err != nil {
			log.Critical("Kmesg Log Sink: %s", err)
		}
	}
	log.Info("Mounting from %s to %s", os.Args[1], os.Args[2])
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
