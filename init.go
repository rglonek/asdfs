package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8"
	"gopkg.in/yaml.v3"
)

type Cfg struct {
	Aerospike struct {
		Host      string `yaml:"host"`
		Port      int    `yaml:"port"`
		Namespace string `yaml:"namespace"`
		Auth      struct {
			Username string `yaml:"username"`
			Password string `yaml:"password"`
			Mode     string `yaml:"mode"`
		} `yaml:"auth"`
		TLS struct {
			CAFile   string `yaml:"caFile"`
			CertFile string `yaml:"certFile"`
			KeyFile  string `yaml:"keyFile"`
			TlsName  string `yaml:"tlsName"`
		} `yaml:"tls"`
		Timeouts cfgTimeout `yaml:"timeouts"`
	} `yaml:"aerospike"`
	FS struct {
		RootMode uint32 `yaml:"rootMode"`
	} `yaml:"fs"`
	MountDir string `yaml:"mountDir"`
	Log      struct {
		Level  int    `yaml:"level"`
		Kmesg  bool   `yaml:"kmesg"`
		Stderr bool   `yaml:"stderr"`
		File   string `yaml:"file"`
	} `yaml:"log"`
	MountParams struct {
		RW    bool `yaml:"rw"`
		RO    bool `yaml:"ro"`
		Debug bool `yaml:"debug"`
	} `yaml:"mountParams"`
}

type cfgTimeout struct {
	Total   time.Duration `yaml:"total"`
	Socket  time.Duration `yaml:"socket"`
	MRT     time.Duration `yaml:"mrt"`
	Connect time.Duration `yaml:"connect"`
	Login   time.Duration `yaml:"login"`
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
	if err != nil {
		return nil, err
	}
	if config.Aerospike.Timeouts.Socket == 0 {
		config.Aerospike.Timeouts.Socket = 30 * time.Second
	}
	if config.Aerospike.Timeouts.Total == 0 {
		config.Aerospike.Timeouts.Total = 120 * time.Second
	}
	if config.Aerospike.Timeouts.MRT == 0 {
		config.Aerospike.Timeouts.MRT = 120 * time.Second
	}
	if config.Aerospike.Timeouts.Connect == 0 {
		config.Aerospike.Timeouts.Connect = 60 * time.Second
	}
	if config.Aerospike.Timeouts.Login == 0 {
		config.Aerospike.Timeouts.Login = 60 * time.Second
	}
	if config.FS.RootMode == 0 {
		config.FS.RootMode = 0o755
	}
	if config.Log.Level == 0 {
		config.Log.Level = 3
	} else if config.Log.Level == -1 {
		config.Log.Level = 0
	}
	if !config.Log.Kmesg && !config.Log.Stderr && !config.Log.Stderr {
		config.Log.Kmesg = true
	}
	return config, nil
}

// build TLS configuration
func buildTLSConfig(tlsName string, caFile string, certFile string, keyFile string) (*tls.Config, error) {
	nTLS := new(tls.Config)
	nTLS.InsecureSkipVerify = true
	nTLS.ServerName = tlsName
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("tls: loadca: %s", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	nTLS.RootCAs = caCertPool
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: loadkeys: %s", err)
		}
		nTLS.Certificates = []tls.Certificate{cert}
	}
	return nTLS, nil
}

func Connect(c *Cfg) (*aerospike.Client, error) {
	// we can add policy items for timeout, retries, creation of sindexes, etc, everything init goes here
	cp := aerospike.NewClientPolicy()
	cp.Timeout = c.Aerospike.Timeouts.Connect
	cp.LoginTimeout = c.Aerospike.Timeouts.Login
	if c.Aerospike.Auth.Username != "" {
		cp.User = c.Aerospike.Auth.Username
		cp.Password = c.Aerospike.Auth.Password
		switch strings.ToUpper(c.Aerospike.Auth.Mode) {
		case "EXTERNAL":
			cp.AuthMode = aerospike.AuthModeExternal
		case "PKI":
			cp.AuthMode = aerospike.AuthModePKI
		case "INTERNAL", "":
			cp.AuthMode = aerospike.AuthModeInternal
		default:
			return nil, errors.New("auth mode not supported")
		}
	}
	if c.Aerospike.TLS.CAFile != "" {
		tlsConfig, err := buildTLSConfig(c.Aerospike.TLS.TlsName, c.Aerospike.TLS.CAFile, c.Aerospike.TLS.CertFile, c.Aerospike.TLS.KeyFile)
		if err != nil {
			return nil, err
		}
		cp.TlsConfig = tlsConfig
	}
	asd, err := aerospike.NewClientWithPolicy(cp, c.Aerospike.Host, c.Aerospike.Port)
	if err != nil {
		return nil, err
	}
	log.Debug("Connected, checking whether filesystem is initialized")
	// get root entry, and if it doesn't exist, create it to initialize the filesystem
	kk, err := aerospike.NewKey(c.Aerospike.Namespace, "fs", 1)
	if err != nil {
		return nil, err
	}
	mrt := GetPolicies(asd, &c.Aerospike.Timeouts)
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
