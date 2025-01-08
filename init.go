package main

import (
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"gopkg.in/yaml.v3"
)

type Cfg struct {
	Aerospike struct {
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		Namespace string `yaml:"namespace"`
	} `yaml:"aerospike"`
	FS struct {
		RootMode uint32 `yaml:"rootMode"`
	} `yaml:"fs"`
	MountDir string `yaml:"mountDir"`
	LogLevel int    `yaml:"logLevel"`
}

func NewConfigFromFile(file string) (*Cfg, error) {
	if _, err := os.Stat(file); err != nil {
		return nil, fmt.Errorf("could not access %s: %s", file, err)
	}
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return NewConfig(f)
}

func NewConfig(conf io.Reader) (*Cfg, error) {
	config := &Cfg{}
	dec := yaml.NewDecoder(conf)
	err := dec.Decode(config)
	return config, err
}

func Connect(c *Cfg) (*aerospike.Client, error) {
	// we can add policy items for timeout, retries, creation of sindexes, etc, everything init goes here
	asd, err := aerospike.NewClient(c.Aerospike.Host, c.Aerospike.Port)
	if err != nil {
		return nil, err
	}
	log.Debug("Connected, checking whether filesystem is initialized")
	// get root entry, and if it doesn't exist, create it to initialize the filesystem
	kk, err := aerospike.NewKey(c.Aerospike.Namespace, "fs", 1)
	if err != nil {
		return nil, err
	}
	mrt := GetPolicies(asd)
	exists, err := asd.Exists(mrt.Read(), kk)
	if err != nil {
		mrt.Abort()
		return asd, err
	}
	if exists {
		log.Debug("File system initialized already")
		mrt.Abort()
		return asd, nil
	}
	log.Debug("Initializing filesystem")
	files := make(Ls)
	bins := make(aerospike.BinMap)
	bins["Ls"] = files.ToAerospikeMap()
	bins["Atime"] = TimeToDB(time.Now())
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["BlockSize"] = 8 * 1024 * 1024
	bins["Blocks"] = 1
	bins["Gid"] = 0
	bins["Uid"] = 0
	bins["Size"] = 8 * 1024 * 1024 // blocks * blocksize
	bins["Rdev"] = 0
	bins["Nlink"] = 1                                          // always 1 for root entry
	bins["Flags"] = 0                                          // no flags for root entry
	bins["Mode"] = iofs.ModeDir | iofs.FileMode(c.FS.RootMode) // default mode for root entry 0o755 ?
	wp := mrt.Write()
	wp.RecordExistsAction = aerospike.CREATE_ONLY
	err = asd.Put(wp, kk, bins)
	if err != nil {
		mrt.Abort()
		return asd, err
	}
	k, err := aerospike.NewKey(c.Aerospike.Namespace, "meta", "lastInode")
	if err != nil {
		mrt.Abort()
		return asd, err
	}
	err = asd.PutBins(wp, k, aerospike.NewBin("lastInode", 1))
	if err != nil {
		mrt.Abort()
		return asd, err
	}
	xerr := mrt.Commit()
	if xerr != nil {
		return asd, xerr
	}
	log.Debug("Filesystem initialization complete")
	return asd, nil
}
