package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	iofs "io/fs"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/aerospike/aerospike-client-go/v7"
)

type cfg struct {
	Aerospike struct {
		Host      string
		Port      int
		Namespace string
	}
	Mount struct {
		Directory   string
		InodeOffset int
	}
}

func NewConfig(args []string) (*cfg, error) {
	config := &cfg{}
	items := strings.Split(args[1], ",")
	for _, i := range items {
		kv := strings.Split(i, "=")
		if len(kv) != 2 {
			fmt.Printf("(1) Usage: %s host=...,port=...,ns=...,offset=... dest/\n", args[0])
			os.Exit(1)
		}
		switch kv[0] {
		case "host":
			config.Aerospike.Host = kv[1]
		case "ns":
			config.Aerospike.Namespace = kv[1]
		case "port":
			var err error
			config.Aerospike.Port, err = strconv.Atoi(kv[1])
			if err != nil {
				fmt.Print("Port must be an integer")
				os.Exit(1)
			}
		case "offset":
			var err error
			config.Mount.InodeOffset, err = strconv.Atoi(kv[1])
			if err != nil {
				fmt.Print("offset must be an integer")
				os.Exit(1)
			}
		}
	}
	if config.Mount.InodeOffset == 0 {
		config.Mount.InodeOffset = 1000000000
	}
	config.Mount.Directory = args[2]
	return config, nil
}

func Connect(c *cfg) (*aerospike.Client, error) {
	// we can add policy items for timeout, retries, creation of sindexes, etc, everything init goes here
	asd, err := aerospike.NewClient(c.Aerospike.Host, c.Aerospike.Port)
	if err != nil {
		return nil, err
	}
	newNode := uint64(1)
	kk, err := aerospike.NewKey(c.Aerospike.Namespace, "fs", int(newNode))
	if err != nil {
		log.Print(err)
		return nil, err
	}
	_, err = asd.Get(nil, kk)
	if err == nil {
		return asd, nil
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	data, _ := json.Marshal(files)
	bins := make(aerospike.BinMap)
	bins["ls"] = string(data)
	bins["Atime"] = time.Now().Format(time.RFC3339)
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["BlockSize"] = 0
	bins["Blocks"] = 0
	bins["Gid"] = 0
	bins["Uid"] = 0
	bins["Size"] = 0
	bins["Rdev"] = 0
	bins["Nlink"] = 1
	bins["Flags"] = 0
	bins["Mode"] = iofs.ModeDir | 0o755
	err = asd.Put(nil, kk, bins)
	if err != nil {
		return asd, err
	}
	k, err := aerospike.NewKey(c.Aerospike.Namespace, "inodes", c.Mount.InodeOffset)
	if err != nil {
		return asd, err
	}
	_, err = asd.Get(nil, k, "lastInode")
	if err != nil {
		newNode := c.Mount.InodeOffset + 1
		err = asd.PutBins(nil, k, aerospike.NewBin("lastInode", newNode))
		if err != nil {
			return asd, err
		}
	}
	return asd, nil
}

func main() {
	os.Setenv("PATH", "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s host=...,port=...,ns=...,offset=... dest/\n", os.Args[0])
		os.Exit(1)
	}
	c, err := NewConfig(os.Args)
	if err != nil {
		log.Fatal(err)
	}
	asd, err := Connect(c)
	if err != nil {
		log.Fatal(err)
	}
	err = mount(c, asd)
	if err != nil {
		log.Fatal(err)
	}
}

func mount(c *cfg, asd *aerospike.Client) error {
	conn, err := fuse.Mount(c.Mount.Directory, fuse.FSName("asd"), fuse.Subtype("asdfs"))
	if err != nil {
		log.Print(err)
		return err
	}
	defer conn.Close()
	server := fs.New(conn, nil)
	filesys := &FS{
		fuse:       server,
		asd:        asd,
		cfg:        c,
		lock:       new(sync.Mutex),
		inodeLocks: make(map[uint64]*sync.Mutex),
	}
	return server.Serve(filesys)
}

type FS struct {
	fuse       *fs.Server
	asd        *aerospike.Client
	cfg        *cfg
	inodeLocks map[uint64]*sync.Mutex
	lock       *sync.Mutex
}

func (f *FS) newInode() (uint64, error) {
	f.lock.Lock()
	defer f.lock.Unlock()
	k, err := aerospike.NewKey(f.cfg.Aerospike.Namespace, "inodes", f.cfg.Mount.InodeOffset)
	if err != nil {
		return 0, err
	}
	r, err := f.asd.Get(nil, k, "lastInode")
	if err != nil {
		return 0, err
	}
	newNode := r.Bins["lastInode"].(int) + 1
	err = f.asd.PutBins(nil, k, aerospike.NewBin("lastInode", newNode))
	if err != nil {
		return 0, err
	}
	return uint64(newNode), nil
}

func (f *FS) lockInode(inode uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.inodeLocks[inode]; !ok {
		f.inodeLocks[inode] = new(sync.Mutex)
	}
	f.inodeLocks[inode].Lock()
}

func (f *FS) unlockInode(inode uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.inodeLocks[inode].Unlock()
}

func (f *FS) removeInodeLock(inode uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()
	delete(f.inodeLocks, inode)
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{fs: f, inode: 1}, nil
}

type Dir struct {
	fs    *FS
	inode uint64
}

func (f *FS) attr(ctx context.Context, a *fuse.Attr, inode uint64) error {
	k, err := aerospike.NewKey(f.cfg.Aerospike.Namespace, "fs", int64(inode))
	if err != nil {
		log.Print(err)
		return err
	}
	r, err := f.asd.Get(nil, k)
	if err != nil {
		log.Print(k, err)
		return err
	}
	a.Inode = inode
	a.Atime, _ = time.Parse(time.RFC3339, r.Bins["Atime"].(string))
	a.BlockSize = uint32(r.Bins["BlockSize"].(int))
	a.Blocks = uint64(r.Bins["Blocks"].(int))
	a.Ctime, _ = time.Parse(time.RFC3339, r.Bins["Ctime"].(string))
	a.Flags = fuse.AttrFlags(uint32(r.Bins["Flags"].(int)))
	a.Gid = uint32(r.Bins["Gid"].(int))
	a.Mode = iofs.FileMode(uint32(r.Bins["Mode"].(int)))
	a.Mtime, _ = time.Parse(time.RFC3339, r.Bins["Mtime"].(string))
	a.Nlink = uint32(r.Bins["Nlink"].(int))
	a.Rdev = uint32(r.Bins["Rdev"].(int))
	a.Size = uint64(r.Bins["Size"].(int))
	a.Uid = uint32(r.Bins["Uid"].(int))
	a.Valid = 1
	return nil
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	return d.fs.attr(ctx, a, d.inode)
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	var err error

	newNode, err := d.fs.newInode()
	if err != nil {
		return nil, err
	}
	kk, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(newNode))
	if err != nil {
		log.Print(err)
		return nil, err
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	data, _ := json.Marshal(files)
	bins := make(aerospike.BinMap)
	bins["ls"] = string(data)
	bins["Atime"] = time.Now().Format(time.RFC3339)
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["BlockSize"] = 0
	bins["Blocks"] = 0
	bins["Gid"] = int(req.Gid)
	bins["Uid"] = int(req.Uid)
	bins["Size"] = 0
	bins["Rdev"] = 0
	bins["Nlink"] = 1
	bins["Flags"] = 0
	bins["Mode"] = int(req.Mode)
	err = d.fs.asd.Put(nil, kk, bins)
	if err != nil {
		log.Print(err)
		return nil, err
	}

	d.fs.lockInode(uint64(d.inode))
	k, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		d.fs.unlockInode(uint64(d.inode))
		log.Print(err)
		return nil, err
	}
	r, err := d.fs.asd.Get(nil, k, "ls")
	if err != nil {
		d.fs.unlockInode(uint64(d.inode))
		log.Print(k, err)
		return nil, err
	}
	files = make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		d.fs.unlockInode(uint64(d.inode))
		log.Print(err)
		return nil, err
	}
	files[req.Name] = struct {
		Inode uint64
		Type  fuse.DirentType
	}{
		Inode: newNode,
		Type:  fuse.DT_Dir,
	}
	data, _ = json.Marshal(files)
	ls := aerospike.NewBin("ls", string(data))
	err = d.fs.asd.PutBins(nil, k, ls)
	if err != nil {
		return nil, err
	}
	d.fs.unlockInode(uint64(d.inode))
	return &Dir{
		fs:    d.fs,
		inode: newNode,
	}, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	return d.fs.remove(ctx, req, d.inode)
}

func (fs *FS) remove(ctx context.Context, req *fuse.RemoveRequest, inode uint64) error {
	var err error
	fs.lockInode(uint64(inode))
	k, err := aerospike.NewKey(fs.cfg.Aerospike.Namespace, "fs", int(inode))
	if err != nil {
		fs.unlockInode(uint64(inode))
		log.Print(err)
		return err
	}
	r, err := fs.asd.Get(nil, k, "ls")
	if err != nil {
		fs.unlockInode(uint64(inode))
		log.Print(k, err)
		return err
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		fs.unlockInode(uint64(inode))
		log.Print(err)
		return err
	}
	delete(files, req.Name)
	data, _ := json.Marshal(files)
	ls := aerospike.NewBin("ls", string(data))
	err = fs.asd.PutBins(nil, k, ls)
	if err != nil {
		return err
	}
	fs.unlockInode(uint64(inode))
	kk, err := aerospike.NewKey(fs.cfg.Aerospike.Namespace, "fs", int(req.Node))
	if err != nil {
		log.Print(err)
		return err
	}
	_, err = fs.asd.Delete(nil, kk)
	if err != nil {
		log.Print(err)
		return err
	}
	fs.removeInodeLock(uint64(req.Node))
	return nil
}

func (fs *FS) rename(ctx context.Context, req *fuse.RenameRequest, inode uint64) error {
	// put this entry in new inode with new name
	fs.lockInode(uint64(req.NewDir))
	var err error
	k, err := aerospike.NewKey(fs.cfg.Aerospike.Namespace, "fs", int(req.NewDir))
	if err != nil {
		fs.unlockInode(uint64(req.NewDir))
		log.Print(err)
		return err
	}
	r, err := fs.asd.Get(nil, k, "ls")
	if err != nil {
		fs.unlockInode(uint64(req.NewDir))
		log.Print(k, err)
		return err
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		fs.unlockInode(uint64(req.NewDir))
		log.Print(err)
		return err
	}
	files[req.NewName] = struct {
		Inode uint64
		Type  fuse.DirentType
	}{
		Inode: inode,
		Type:  fuse.DT_Dir,
	}
	data, _ := json.Marshal(files)
	ls := aerospike.NewBin("ls", string(data))
	err = fs.asd.PutBins(nil, k, ls)
	if err != nil {
		return err
	}
	fs.unlockInode(uint64(req.NewDir))
	// remove this entry from old inode with old name
	fs.lockInode(uint64(req.Node))
	k, err = aerospike.NewKey(fs.cfg.Aerospike.Namespace, "fs", int(req.Node))
	if err != nil {
		fs.unlockInode(uint64(req.Node))
		log.Print(err)
		return err
	}
	r, err = fs.asd.Get(nil, k, "ls")
	if err != nil {
		fs.unlockInode(uint64(req.Node))
		log.Print(k, err)
		return err
	}
	files = make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		fs.unlockInode(uint64(req.Node))
		log.Print(err)
		return err
	}
	delete(files, req.OldName)
	data, _ = json.Marshal(files)
	ls = aerospike.NewBin("ls", string(data))
	err = fs.asd.PutBins(nil, k, ls)
	if err != nil {
		return err
	}
	fs.unlockInode(uint64(req.Node))
	return nil
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest) error {
	return d.fs.rename(ctx, req, d.inode)
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	var err error
	k, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		return nil, err
	}
	r, err := d.fs.asd.Get(nil, k, "ls")
	if err != nil {
		return nil, fmt.Errorf("%s %s", k, err)
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		return nil, err
	}
	ent, ok := files[name]
	if !ok {
		return nil, syscall.ENOENT
	}
	switch ent.Type {
	case fuse.DT_Dir:
		return &Dir{
			fs:    d.fs,
			inode: ent.Inode,
		}, nil
	case fuse.DT_File:
		return &File{
			fs:    d.fs,
			inode: ent.Inode,
		}, nil
	default:
		return nil, errors.New("unsupported type")
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	ret := []fuse.Dirent{}
	var err error
	k, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		return nil, err
	}
	r, err := d.fs.asd.Get(nil, k, "ls")
	if err != nil {
		return nil, fmt.Errorf("%s %s", k, err)
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		return nil, err
	}
	for path, ent := range files {
		ret = append(ret, fuse.Dirent{
			Inode: ent.Inode,
			Name:  path,
			Type:  ent.Type,
		})
	}
	return ret, nil
}

type File struct {
	fs    *FS
	inode uint64
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	return f.fs.attr(ctx, a, f.inode)
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags = fuse.OpenDirectIO
	if err := f.fs.fuse.InvalidateNodeData(f); err != nil && err != fuse.ErrNotCached {
		log.Printf("invalidate error: %v", err)
	}
	return f, nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	k, err := aerospike.NewKey(f.fs.cfg.Aerospike.Namespace, "fs", int(f.inode))
	if err != nil {
		log.Print(err)
		return err
	}
	r, err := f.fs.asd.Get(nil, k, "data")
	if err != nil {
		log.Print(k, err)
		return err
	}
	fuseutil.HandleRead(req, resp, r.Bins["data"].([]byte))
	return nil
}

func (f *File) Rename(ctx context.Context, req *fuse.RenameRequest) error {
	return f.fs.rename(ctx, req, f.inode)
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	k, err := aerospike.NewKey(f.fs.cfg.Aerospike.Namespace, "fs", int(f.inode))
	if err != nil {
		log.Print(err)
		return err
	}
	_, err = f.fs.asd.Get(nil, k, "Size")
	if err != nil {
		log.Print(k, err)
		return err
	}
	err = f.fs.asd.PutBins(nil, k, aerospike.NewBin("data", req.Data), aerospike.NewBin("Size", len(req.Data)))
	if err != nil {
		return err
	}
	resp.Size = len(req.Data)
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	var err error
	resp.Flags = fuse.OpenDirectIO
	if err := d.fs.fuse.InvalidateNodeData(d); err != nil && err != fuse.ErrNotCached {
		log.Printf("invalidate error: %v", err)
	}
	newNode, err := d.fs.newInode()
	if err != nil {
		return nil, nil, err
	}
	kk, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(newNode))
	if err != nil {
		log.Print(err)
		return nil, nil, err
	}
	files := make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	data := []byte{}
	bins := make(aerospike.BinMap)
	bins["data"] = string(data)
	bins["Atime"] = time.Now().Format(time.RFC3339)
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["BlockSize"] = 0
	bins["Blocks"] = 0
	bins["Gid"] = int(req.Gid)
	bins["Uid"] = int(req.Uid)
	bins["Size"] = 0
	bins["Rdev"] = 0
	bins["Nlink"] = 1
	bins["Flags"] = 0
	bins["Mode"] = int(req.Mode)
	err = d.fs.asd.Put(nil, kk, bins)
	if err != nil {
		log.Print(err)
		return nil, nil, err
	}

	d.fs.lockInode(uint64(d.inode))
	k, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		d.fs.unlockInode(uint64(d.inode))
		log.Print(err)
		return nil, nil, err
	}
	r, err := d.fs.asd.Get(nil, k, "ls")
	if err != nil {
		d.fs.unlockInode(uint64(d.inode))
		log.Print(k, err)
		return nil, nil, err
	}
	files = make(map[string]struct {
		Inode uint64
		Type  fuse.DirentType
	})
	err = json.Unmarshal([]byte(r.Bins["ls"].(string)), &files)
	if err != nil {
		d.fs.unlockInode(uint64(d.inode))
		log.Print(err)
		return nil, nil, err
	}
	files[req.Name] = struct {
		Inode uint64
		Type  fuse.DirentType
	}{
		Inode: newNode,
		Type:  fuse.DT_File,
	}
	data, _ = json.Marshal(files)
	ls := aerospike.NewBin("ls", string(data))
	err = d.fs.asd.PutBins(nil, k, ls)
	if err != nil {
		return nil, nil, err
	}
	d.fs.unlockInode(uint64(d.inode))
	return &File{
			fs:    d.fs,
			inode: newNode,
		}, &File{
			fs:    d.fs,
			inode: newNode,
		}, nil
}
