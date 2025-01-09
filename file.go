package main

import (
	"context"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/aerospike/aerospike-client-go/v8"
)

func (f *File) truncate(mrt *MRT) error {
	if f.fs.cfg.MountParams.RO {
		return syscall.EROFS
	}
	log.Detail("Truncating %d on request from flags", f.inode)
	k, err := aerospike.NewKey(f.fs.cfg.Aerospike.Namespace, "fs", int(f.inode))
	if err != nil {
		return err
	}
	err = f.fs.asd.PutBins(mrt.Write(), k, aerospike.NewBin("data", []byte{}), aerospike.NewBin("Size", 0), aerospike.NewBin("Mtime", TimeToDB(time.Now())), aerospike.NewBin("Atime", TimeToDB(time.Now())))
	if err != nil {
		return err
	}
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	err := f.fs.setattr(ctx, req, resp, f.inode)
	if err != nil {
		log.Error("Inode %d SetAttr: %s", f.inode, err)
		return err
	}
	return nil
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	err := f.fs.attr(ctx, a, f.inode)
	if err != nil {
		log.Error("Inode %d Attr: %s", f.inode, err)
		return err
	}
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	log.Debug("Executing Open %d Flags:%v OpenFlags:%v", f.inode, req.Flags, req.OpenFlags)
	resp.Flags = fuse.OpenDirectIO
	if err := f.fs.fuse.InvalidateNodeData(f); err != nil && err != fuse.ErrNotCached {
		log.Warn("invalidate error: %v", err)
	}
	nHandle := &File{
		fs:    f.fs,
		inode: f.inode,
		flags: req.Flags,
	}
	if req.Flags&fuse.OpenTruncate != 0 {
		mrt := GetWritePolicy(f.fs.asd)
		err := nHandle.truncate(mrt)
		if err != nil {
			mrt.Abort()
			log.Error("Open: Failed to truncate %d: %s", f.inode, err)
			return nil, syscall.EFAULT
		}
		mrt.Commit()
	}
	return nHandle, nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	log.Debug("Executing Read %d", f.inode)
	if f.flags&fuse.OpenWriteOnly != 0 {
		log.Debug("Read %d: opened write only", f.inode)
		return syscall.EACCES
	}
	k, err := aerospike.NewKey(f.fs.cfg.Aerospike.Namespace, "fs", int(f.inode))
	if err != nil {
		log.Error("Inode %d Read: %s", f.inode, err)
		return syscall.EFAULT
	}
	r, err := f.fs.asd.Get(nil, k, "data")
	if err != nil {
		if err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
			log.Detail("Inode %d Read: not found", f.inode)
			return syscall.ENOENT
		}
		log.Error("Inode %d Read: %s", f.inode, err)
		return syscall.EFAULT
	}
	fuseutil.HandleRead(req, resp, r.Bins["data"].([]byte))
	return nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if f.fs.cfg.MountParams.RO {
		return syscall.EROFS
	}
	log.Debug("Executing Write %d", f.inode)
	if f.flags == 0 {
		log.Error("Write %d: not a handle", f.inode)
		return syscall.EBADF
	}
	if f.flags&fuse.OpenReadOnly != 0 {
		log.Debug("Write %d: opened read only", f.inode)
		return syscall.EACCES
	}
	k, err := aerospike.NewKey(f.fs.cfg.Aerospike.Namespace, "fs", int(f.inode))
	if err != nil {
		log.Error("Inode %d Write: %s", f.inode, err)
		return syscall.EFAULT
	}
	mrt := GetPolicies(f.fs.asd)
	binNames := []string{"Size"}
	if f.flags&fuse.OpenAppend != 0 {
		binNames = append(binNames, "data")
	}
	d, err := f.fs.asd.Get(mrt.Read(), k, binNames...)
	if err != nil {
		mrt.Abort()
		if err.Matches(aerospike.ErrKeyNotFound.ResultCode) {
			log.Detail("Inode %d Read: not found", f.inode)
			return syscall.ENOENT
		}
		log.Error("Inode %d Write: %s", f.inode, err)
		return syscall.EFAULT
	}
	data := req.Data
	dataSize := len(req.Data)
	// if flag OpenAppend
	if f.flags&fuse.OpenAppend != 0 {
		dataSize = d.Bins["Size"].(int) + dataSize
		data = append(d.Bins["data"].([]byte), data...)
	}
	// store
	err = f.fs.asd.PutBins(mrt.Write(), k, aerospike.NewBin("data", data), aerospike.NewBin("Size", dataSize), aerospike.NewBin("Mtime", TimeToDB(time.Now())), aerospike.NewBin("Atime", TimeToDB(time.Now())))
	if err != nil {
		mrt.Abort()
		log.Error("Inode %d Write: %s", f.inode, err)
		return syscall.EFAULT
	}
	xerr := mrt.Commit()
	if xerr != nil {
		log.Error("Inode %d Write: %s", f.inode, xerr)
		return syscall.EFAULT
	}
	resp.Size = len(req.Data)
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	if d.fs.cfg.MountParams.RO {
		return nil, nil, syscall.EROFS
	}
	log.Debug("Executing Create '%s' in %d", req.Name, d.inode)
	// clear cache
	resp.Flags = fuse.OpenDirectIO
	if err := d.fs.fuse.InvalidateNodeData(d); err != nil && err != fuse.ErrNotCached {
		log.Warn("invalidate error: %v", err)
	}
	// check if the file already exists
	parentKey, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, err)
		return nil, nil, syscall.EFAULT
	}
	mrt := GetWritePolicy(d.fs.asd)
	r, err := d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapGetByKeyOp("Ls", req.Name, aerospike.MapReturnType.VALUE))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, err)
		return nil, nil, syscall.EFAULT
	}
	res := r.Bins["Ls"]
	if res != nil {
		// already exists
		// if it's a dir, error
		if res.(map[interface{}]interface{})["Type"].(int) == int(fuse.DT_Dir) {
			log.Error("Parent %d Create '%s': exists, is dir", d.inode, req.Name)
			mrt.Abort()
			return nil, nil, syscall.EEXIST
		}
		if req.Flags&fuse.OpenCreate != 0 {
			// we are just opening the file as it exists
			// if Truncate, override the file contents data bin with empty - truncate
			nHandle := &File{
				fs:    d.fs,
				inode: uint64(res.(map[interface{}]interface{})["Inode"].(int)),
				flags: req.Flags,
			}
			if req.Flags&fuse.OpenTruncate != 0 {
				err := nHandle.truncate(mrt)
				if err != nil {
					log.Error("Open: Failed to truncate %d: %s", nHandle.inode, err)
					return nil, nil, syscall.EFAULT
				}
			}
			mrt.Commit()
			return nHandle, nHandle, nil
		}
		// file already exists: error
		mrt.Abort()
		return nil, nil, syscall.EEXIST
	}
	// obtain new inode, advancing lastInode metadata record
	newNode, xerr := d.fs.newInode(mrt.txn)
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, xerr)
		return nil, nil, syscall.EFAULT
	}
	// create new fs entry with new inode - our new file
	kk, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(newNode))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, err)
		return nil, nil, syscall.EFAULT
	}
	data := []byte{}
	bins := make(aerospike.BinMap)
	bins["data"] = data
	bins["Atime"] = TimeToDB(time.Now())
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["BlockSize"] = 8 * 1024 * 1024
	bins["Blocks"] = 1
	bins["Gid"] = int(req.Gid)
	bins["Uid"] = int(req.Uid)
	bins["Size"] = 0
	bins["Rdev"] = 0
	bins["Nlink"] = 1
	bins["Flags"] = 0
	bins["Mode"] = int(req.Mode)
	log.Detail("Parent %d Create '%s': %v req.Umask:%d req.Flags:%v", d.inode, req.Name, bins, req.Umask, req.Flags)
	err = d.fs.asd.Put(mrt.Write(), kk, bins)
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, err)
		return nil, nil, syscall.EFAULT
	}
	// update `ls` of directory entry, indicating we have a new file there
	mp := aerospike.NewMapPolicy(aerospike.MapOrder.KEY_ORDERED, aerospike.MapWriteMode.CREATE_ONLY)
	lsVal := &LsItem{
		Inode: uint64(newNode),
		Type:  fuse.DT_File,
	}
	_, err = d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapPutOp(mp, "Ls", req.Name, lsVal.ToAerospikeMap()), aerospike.PutOp(aerospike.NewBin("Mtime", TimeToDB(time.Now()))), aerospike.PutOp(aerospike.NewBin("Atime", TimeToDB(time.Now()))))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, err)
		return nil, nil, syscall.EFAULT
	}
	xerr = mrt.Commit()
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Create '%s': %s", d.inode, req.Name, xerr)
		return nil, nil, syscall.EFAULT
	}
	// return node and handle
	nHandle := &File{
		fs:    d.fs,
		inode: uint64(newNode),
		flags: req.Flags,
	}
	return nHandle, nHandle, nil
}
