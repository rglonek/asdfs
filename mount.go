package main

import (
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aerospike/aerospike-client-go/v8"
)

func Mount(c *Cfg, asd *aerospike.Client) error {
	conn, err := fuse.Mount(c.MountDir, fuse.FSName("asd"), fuse.Subtype("asdfs"))
	if err != nil {
		return err
	}
	defer conn.Close()
	server := fs.New(conn, nil)
	filesys := &FS{
		fuse: server,
		asd:  asd,
		cfg:  c,
	}
	return server.Serve(filesys)
}
