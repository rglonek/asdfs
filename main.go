package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/aerospike/aerospike-client-go/v8"
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
	log.SetPrefix("asd-fs: ")
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
	log.Info("Adding signal handlers")
	sigHandler(asd)
	log.Info("Executing Mount")
	err = Mount(c, asd)
	log.Info("Waiting for all writes to complete")
	cleanup()
	if err != nil {
		log.Critical("%s", err)
	}
	log.Info("Exiting")
}

var oplock = new(sync.Mutex) // each write op will attempt an oplock.Lock(),Unlock() before continuing
var ops = new(sync.RWMutex)  // each write op will perform an ops.RLock() and RUnlock() when done

func OpStart() {
	oplock.Lock()
	ops.RLock()
	oplock.Unlock()
}

func OpEnd() {
	ops.RUnlock()
}

func cleanup() {
	oplock.Lock() // lock a lock so that no more write operations can be done
	ops.Lock()    // wait for existing operations to complete - if we can lock this one, that means all ops have completed and released their RLocks
}

// add a sigint/sigterm handler which will call cleanup() and then exit the process
func sigHandler(asd *aerospike.Client) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		log.Info("Received signal: %v, waiting for all writes to complete before exit", sig)
		cleanup()
		log.Info("Exiting")
		asd.Close()
		os.Exit(0)
	}()
}
