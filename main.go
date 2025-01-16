package main

import (
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/rglonek/logger"
	"gopkg.in/yaml.v3"
)

var log = logger.NewLogger()

func main() {
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s /path/to/config.yaml dest/\n", os.Args[0])
		os.Exit(1)
	}

	if d, err := os.Stat(os.Args[2]); err != nil || !d.IsDir() {
		log.Critical("Mount point directory does not exist or is not a directory")
	}

	c, err := NewConfigFromFile(os.Args[1])
	if err != nil {
		log.Critical("%s", err)
	}
	c.MountDir = os.Args[2]

	if len(os.Args) >= 5 {
		if os.Args[3] != "-o" {
			log.Critical("Invalid argument (%v)", os.Args)
		}
		for _, param := range strings.Split(strings.ToLower(os.Args[4]), ",") {
			switch param {
			case "rw":
				c.MountParams.RW = true
				c.MountParams.RO = false
			case "ro":
				c.MountParams.RW = false
				c.MountParams.RO = true
			case "debug":
				c.MountParams.Debug = true
				c.Log.Stderr = true
			}
		}
	}

	if c.MountParams.Debug {
		yaml.NewEncoder(os.Stderr).Encode(c)
	}

	if !c.MountParams.Debug && os.Getenv("ASDFS_BG") == "" {
		// launch in background with the env var set and exit
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		cmd.Env = append(os.Environ(), "ASDFS_BG=1")
		err = cmd.Start()
		if err != nil {
			log.Critical("Failed to start background process! Exiting (%s)", err)
		}
		// wait for child to finish initializing or die
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGUSR1)
		go func() {
			<-sigs
			// init finished, release child and exit
			err = cmd.Process.Release()
			if err != nil {
				log.Critical("Failed to release background process! Exiting (%s)", err)
			}
			os.Exit(0)
		}()
		// wait for the child to die, if it does, exit with an error
		cmd.Wait()
		os.Exit(1)
	}

	log.SetLogLevel(c.Log.Level)
	log.SetPrefix("asd-fs: ")
	if !c.Log.Stderr || !c.MountParams.Debug {
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
	log.Info("Init mount system")
	conn, err := fuse.Mount(c.MountDir, fuse.FSName("asd"), fuse.Subtype("asdfs"))
	if err != nil {
		log.Critical("%s", err)
	}
	defer conn.Close()
	if !c.MountParams.Debug {
		log.Info("Detaching")
		detach()
		// Send signal to parent
		parentPID := os.Getppid()
		parentProc, _ := os.FindProcess(parentPID)
		parentProc.Signal(syscall.SIGUSR1)
	}
	log.Info("Executing Mount")

	server := fs.New(conn, nil)
	filesys := &FS{
		fuse: server,
		asd:  asd,
		cfg:  c,
	}
	err = server.Serve(filesys)

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

func detach() {
	// debug isn't set, which means we have backgrounded, create empty std* sinks
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		log.Critical("Failed to open devnull")
	}

	// Redirect os.Stdout and os.Stderr to /dev/null
	os.Stdout = devNull
	os.Stderr = devNull
	os.Stdin = devNull
}
