package main

import (
	"context"
	"os"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aerospike/aerospike-client-go/v8"
)

func (d *Dir) Symlink(ctx context.Context, req *fuse.SymlinkRequest) (fs.Node, error) {
	log.Debug("Creating symlink: dir=%d, name=%s, target=%s\n", d.inode, req.NewName, req.Target)

	if d.fs.cfg.MountParams.RO {
		return nil, syscall.EROFS
	}
	// clear cache
	if err := d.fs.fuse.InvalidateNodeData(d); err != nil && err != fuse.ErrNotCached {
		log.Warn("invalidate error: %v", err)
	}
	// check if the file already exists
	parentKey, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, err)
		return nil, syscall.EFAULT
	}
	mrt := GetWritePolicy(d.fs.asd, &d.fs.cfg.Aerospike.Timeouts)
	r, err := d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapGetByKeyOp("Ls", req.NewName, aerospike.MapReturnType.VALUE))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, err)
		return nil, syscall.EFAULT
	}
	res := r.Bins["Ls"]
	if res != nil {
		// already exists
		log.Error("Parent %d Symlink '%s': exists, is dir", d.inode, req.NewName)
		mrt.Abort()
		return nil, syscall.EEXIST
	}
	// obtain new inode, advancing lastInode metadata record
	newNode, xerr := d.fs.newInode(mrt.txn)
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, xerr)
		return nil, syscall.EFAULT
	}
	// create new fs entry with new inode - our new file
	kk, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(newNode))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, err)
		return nil, syscall.EFAULT
	}
	bins := make(aerospike.BinMap)
	bins["target"] = req.Target
	bins["Gid"] = int(req.Gid)
	bins["Uid"] = int(req.Uid)
	bins["Size"] = len(req.Target)
	bins["Nlink"] = 1
	bins["Flags"] = 0
	bins["Atime"] = TimeToDB(time.Now())
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["Mode"] = int(os.ModeSymlink) | 0o777
	log.Detail("Parent %d Symlink '%s': %v", d.inode, req.NewName, bins)
	err = d.fs.asd.Put(mrt.Write(), kk, bins)
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, err)
		return nil, syscall.EFAULT
	}
	// update `ls` of directory entry, indicating we have a new file there
	mp := aerospike.NewMapPolicy(aerospike.MapOrder.KEY_ORDERED, aerospike.MapWriteMode.CREATE_ONLY)
	lsVal := &LsItem{
		Inode: uint64(newNode),
		Type:  fuse.DT_Link,
	}
	_, err = d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapPutOp(mp, "Ls", req.NewName, lsVal.ToAerospikeMap()), aerospike.PutOp(aerospike.NewBin("Mtime", TimeToDB(time.Now()))), aerospike.PutOp(aerospike.NewBin("Atime", TimeToDB(time.Now()))))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, err)
		return nil, syscall.EFAULT
	}
	xerr = mrt.Commit()
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Symlink '%s': %s", d.inode, req.NewName, xerr)
		return nil, syscall.EFAULT
	}
	// return node and handle
	nHandle := &Symlink{
		fs:    d.fs,
		inode: uint64(newNode),
	}
	return nHandle, nil

}

func (s *Symlink) Readlink(ctx context.Context, req *fuse.ReadlinkRequest) (string, error) {
	// Return the target path of the symlink
	log.Debug("Running Readlink %d", s.inode)
	kk, err := aerospike.NewKey(s.fs.cfg.Aerospike.Namespace, "fs", int(s.inode))
	if err != nil {
		log.Error("Readlink %d: %s", s.inode, err)
		return "", syscall.EFAULT
	}
	r, err := s.fs.asd.Get(GetReadPolicyNoMRT(s.fs.asd, &s.fs.cfg.Aerospike.Timeouts), kk, "target")
	if err != nil {
		log.Error("Readlink %d: %s", s.inode, err)
		return "", syscall.EFAULT
	}
	return r.Bins["target"].(string), nil
}

func (s *Symlink) Attr(ctx context.Context, a *fuse.Attr) error {
	log.Debug("Running LAttr %d", s.inode)

	kk, err := aerospike.NewKey(s.fs.cfg.Aerospike.Namespace, "fs", int(s.inode))
	if err != nil {
		log.Error("LAttr %d: %s", s.inode, err)
		return syscall.EFAULT
	}
	r, err := s.fs.asd.Get(GetReadPolicyNoMRT(s.fs.asd, &s.fs.cfg.Aerospike.Timeouts), kk, "Mode", "Size", "Uid", "Gid", "Atime", "Mtime", "Ctime")
	if err != nil {
		log.Error("LAttr %d: %s", s.inode, err)
		return syscall.EFAULT
	}

	a.Mode = os.FileMode(r.Bins["Mode"].(int))
	a.Size = uint64(r.Bins["Size"].(int))
	a.Inode = s.inode
	a.Gid = uint32(r.Bins["Uid"].(int))
	a.Uid = uint32(r.Bins["Gid"].(int))
	a.Atime = DBToTime(r.Bins["Atime"].(string))
	a.Mtime = DBToTime(r.Bins["Mtime"].(string))
	a.Ctime = DBToTime(r.Bins["Ctime"].(string))
	return nil
}
