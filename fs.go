package main

import (
	"context"
	iofs "io/fs"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aerospike/aerospike-client-go/v8"
)

type FS struct {
	fuse *fs.Server
	asd  *aerospike.Client
	cfg  *Cfg
}

type Dir struct {
	fs    *FS
	inode uint64
}

type File struct {
	fs    *FS
	inode uint64
	flags fuse.OpenFlags
}

type Ls map[string]LsItem

type LsItem struct {
	Inode uint64
	Type  fuse.DirentType
}

func (l *Ls) ToAerospikeMap() map[string]map[string]int {
	ret := make(map[string]map[string]int)
	for k, v := range *l {
		ret[k] = v.ToAerospikeMap()
	}
	return ret
}

func (l *LsItem) ToAerospikeMap() map[string]int {
	ret := make(map[string]int)
	ret["Inode"] = int(l.Inode)
	ret["Type"] = int(l.Type)
	return ret
}

func TimeToDB(t time.Time) string {
	return t.Format(time.RFC3339)
}

func DBToTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{fs: f, inode: 1}, nil
}

func (f *FS) attr(ctx context.Context, a *fuse.Attr, inode uint64) error {
	log.Debug("Getting attr for inode %d", inode)
	k, err := aerospike.NewKey(f.cfg.Aerospike.Namespace, "fs", int64(inode))
	if err != nil {
		return err
	}
	r, err := f.asd.Get(nil, k)
	if err != nil {
		return err
	}
	a.Inode = inode
	a.Atime = DBToTime(r.Bins["Atime"].(string))
	a.BlockSize = uint32(r.Bins["BlockSize"].(int))
	a.Blocks = uint64(r.Bins["Blocks"].(int))
	a.Ctime = DBToTime(r.Bins["Ctime"].(string))
	a.Flags = fuse.AttrFlags(uint32(r.Bins["Flags"].(int)))
	a.Gid = uint32(r.Bins["Gid"].(int))
	a.Mode = iofs.FileMode(uint32(r.Bins["Mode"].(int)))
	a.Mtime = DBToTime(r.Bins["Mtime"].(string))
	a.Nlink = uint32(r.Bins["Nlink"].(int))
	a.Rdev = uint32(r.Bins["Rdev"].(int))
	a.Size = uint64(r.Bins["Size"].(int))
	a.Uid = uint32(r.Bins["Uid"].(int))
	a.Valid = 1
	return nil
}

func (f *FS) newInode(txn *aerospike.Txn) (newNode int, err error) {
	log.Detail("Getting new inode allocation")
	k, err := aerospike.NewKey(f.cfg.Aerospike.Namespace, "meta", "lastInode")
	if err != nil {
		return -1, err
	}
	rp := aerospike.NewPolicy()
	rp.Txn = txn

	lastInode, err := f.asd.Get(rp, k)
	if err != nil {
		return -1, err
	}
	newNode = lastInode.Bins["lastInode"].(int)
	newNode++

	wp := aerospike.NewWritePolicy(0, 0)
	wp.Txn = txn
	err = f.asd.PutBins(wp, k, aerospike.NewBin("lastInode", newNode))
	if err != nil {
		return -1, err
	}
	log.Detail("New inode: %d", newNode)
	return newNode, nil
}
