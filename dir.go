package main

import (
	"context"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/aerospike/aerospike-client-go/v8"
)

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	err := d.fs.attr(ctx, a, d.inode)
	if err != nil {
		log.Error("Inode %d Attr: %s", d.inode, err)
		return err
	}
	return nil
}

func (d *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	err := d.fs.setattr(ctx, req, resp, d.inode)
	if err != nil {
		log.Error("Inode %d SetAttr: %s", d.inode, err)
		return err
	}
	return nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	if d.fs.cfg.MountParams.RO {
		return nil, syscall.EROFS
	}
	log.Debug("Executing Mkdir")
	// check `Ls` to ensure the new entry doesn't already exist
	if err := d.fs.fuse.InvalidateNodeData(d); err != nil && err != fuse.ErrNotCached {
		log.Warn("invalidate error: %v", err)
	}
	// check if the file already exists
	parentKey, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, err)
		return nil, syscall.EFAULT
	}
	mrt := GetWritePolicy(d.fs.asd, &d.fs.cfg.Aerospike.Timeouts)
	r, err := d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapGetByKeyOp("Ls", req.Name, aerospike.MapReturnType.VALUE))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, err)
		return nil, syscall.EFAULT
	}
	res := r.Bins["Ls"]
	if res != nil {
		// already exists
		log.Error("Parent %d Mkdir '%s': exists", d.inode, req.Name)
		mrt.Abort()
		return nil, syscall.EEXIST
	}
	// obtain new inode, advancing lastInode meta entry
	newNode, xerr := d.fs.newInode(mrt.txn)
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, xerr)
		return nil, syscall.EFAULT
	}
	// store the new inode entry - new directory
	files := make(Ls)
	bins := make(aerospike.BinMap)
	bins["Ls"] = files.ToAerospikeMap()
	bins["Atime"] = TimeToDB(time.Now())
	bins["Ctime"] = bins["Atime"]
	bins["Mtime"] = bins["Ctime"]
	bins["BlockSize"] = 8 * 1024 * 1024
	bins["Blocks"] = 1
	bins["Gid"] = int(req.Gid)
	bins["Uid"] = int(req.Uid)
	bins["Size"] = 8 * 1024 * 1024 // blocks * blocksize
	bins["Rdev"] = 0
	bins["Nlink"] = 1
	bins["Flags"] = 0
	bins["Mode"] = int(req.Mode)
	wp := mrt.Write()
	wp.RecordExistsAction = aerospike.CREATE_ONLY
	kk, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", newNode)
	if err != nil {
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, err)
		return nil, syscall.EFAULT
	}
	err = d.fs.asd.Put(wp, kk, bins)
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, err)
		return nil, syscall.EFAULT
	}
	// update the `Ls` of current dir, adding the new entry to the list
	wp.RecordExistsAction = aerospike.UPDATE
	mp := aerospike.NewMapPolicy(aerospike.MapOrder.KEY_ORDERED, aerospike.MapWriteMode.CREATE_ONLY)
	lsVal := &LsItem{
		Inode: uint64(newNode),
		Type:  fuse.DT_Dir,
	}
	_, err = d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapPutOp(mp, "Ls", req.Name, lsVal.ToAerospikeMap()), aerospike.PutOp(aerospike.NewBin("Mtime", TimeToDB(time.Now()))), aerospike.PutOp(aerospike.NewBin("Atime", TimeToDB(time.Now()))))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, err)
		return nil, syscall.EFAULT
	}
	xerr = mrt.Commit()
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Mkdir '%s': %s", d.inode, req.Name, xerr)
		return nil, syscall.EFAULT
	}
	// return new dir entry
	return &Dir{
		fs:    d.fs,
		inode: uint64(newNode),
	}, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if d.fs.cfg.MountParams.RO {
		return syscall.EROFS
	}
	mrt := GetPolicies(d.fs.asd, &d.fs.cfg.Aerospike.Timeouts)
	return d.remove(ctx, req, mrt)
}
func (d *Dir) remove(ctx context.Context, req *fuse.RemoveRequest, mrt *MRT) error {
	log.Debug("Executing Remove %s from %d", req.Name, d.inode)
	var err error
	parentKey, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		log.Error("Parent %d Remove '%s': %s", d.inode, req.Name, err)
		return syscall.EFAULT
	}
	// check if the requested removal is a dir, if so, check if it has items in `Ls`; if so, error dir not empty, cannot delete
	nType, inode, err := d.lookup(ctx, req.Name, mrt.Write())
	if err == syscall.ENOENT {
		mrt.Abort()
		return nil
	}
	if err != nil {
		log.Error("Remove %s from %d: %s", req.Name, d.inode, err)
		mrt.Abort()
		return syscall.EFAULT
	}
	if nType == fuse.DT_Dir {
		dd := &Dir{
			fs:    d.fs,
			inode: inode,
		}
		res, err := dd.readDirAll(ctx, mrt.Read())
		if err != nil {
			log.Error("Remove %s from %d: %s", req.Name, d.inode, err)
			mrt.Abort()
			return syscall.EFAULT
		}
		if len(res) > 0 {
			log.Detail("Failing to remove %s from %d: not empty", req.Name, d.inode)
			mrt.Abort()
			return syscall.ENOTEMPTY
		}
	}
	// update the `Ls` entry, removing the requested file/dir
	_, err = d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapRemoveByKeyOp("Ls", req.Name, aerospike.MapReturnType.NONE), aerospike.PutOp(aerospike.NewBin("Mtime", TimeToDB(time.Now()))), aerospike.PutOp(aerospike.NewBin("Atime", TimeToDB(time.Now()))))
	if err != nil {
		mrt.Abort()
		log.Error("Parent %d Remove '%s': %s", d.inode, req.Name, err)
		return syscall.EFAULT
	}
	// delete the record in question
	kk, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(inode))
	if err != nil {
		log.Error("Remove %s from %d: %s", req.Name, d.inode, err)
		mrt.Abort()
		return syscall.EFAULT
	}
	_, err = d.fs.asd.Delete(mrt.Write(), kk)
	if err != nil {
		log.Error("Remove %s from %d: %s", req.Name, d.inode, err)
		mrt.Abort()
		return syscall.EFAULT
	}
	// done
	xerr := mrt.Commit()
	if xerr != nil {
		mrt.Abort()
		log.Error("Parent %d Remove '%s': %s", d.inode, req.Name, xerr)
		return syscall.EFAULT
	}
	return nil
}

// if d.inode->req.OldName is a dir, if req.NewDir(Ls)->req.NewName exists, error
// if d.inode->req.OldName is a file, if req.NewDir(Ls)->req.NewName exists:
//
//	if req.NewDir(Ls)->req.NewName is a dir: error
//	if req.NewDir(Ls)->req.NewName is a file: delete the file, it is getting overwritten
//
// from d.inode(Ls) remove req.OldName
// add req.NewName to req.NewDir(Ls)
func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	if d.fs.cfg.MountParams.RO {
		return syscall.EROFS
	}
	log.Debug("Executing Rename %s->%s on %d->%d", req.OldName, req.NewName, d.inode, req.NewDir)
	mrt := GetPolicies(d.fs.asd, &d.fs.cfg.Aerospike.Timeouts)
	// lookup Old
	otype, oinode, err := d.lookup(ctx, req.OldName, mrt.Write())
	if err != nil {
		mrt.Abort()
		log.Error("Rename %s->%s on %d->%d: lookup old: %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
		return err
	}
	// lookup New
	nd := &Dir{
		fs:    d.fs,
		inode: uint64(req.NewDir),
	}
	ntype, ninode, err := nd.lookup(ctx, req.NewName, mrt.Write())
	if err != nil && err != syscall.ENOENT {
		mrt.Abort()
		log.Error("Rename %s->%s on %d->%d: lookup new: %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
		return err
	}
	// if it's a dir, and destination exists, just error
	if otype == fuse.DT_Dir && ninode != 0 {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: src=dir dst=EEXXIST", req.OldName, req.NewName, d.inode, req.NewDir)
		return syscall.EEXIST
	}
	// if it's a file and new(exists and dir), error
	if otype == fuse.DT_File && ninode != 0 && ntype == fuse.DT_Dir {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: src=file dst=EEXXIST+DT_DIR", req.OldName, req.NewName, d.inode, req.NewDir)
		return syscall.EEXIST
	}
	// if it's a file and new(exists, file), delete the new - it is getting overwritten
	if otype == fuse.DT_File && ninode != 0 && ntype == fuse.DT_File {
		err = nd.remove(ctx, &fuse.RemoveRequest{
			Name: req.NewName,
		}, mrt)
		if err != nil {
			mrt.Abort()
			log.Detail("Rename %s->%s on %d->%d: delete dest file: %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
			return err
		}
	}
	// from d.inode(Ls) remove req.OldName
	oldKey, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: NewKey(old): %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
		return syscall.EFAULT
	}
	_, err = d.fs.asd.Operate(mrt.Write(), oldKey, aerospike.MapRemoveByKeyOp("Ls", req.OldName, aerospike.MapReturnType.NONE), aerospike.PutOp(aerospike.NewBin("Mtime", TimeToDB(time.Now()))), aerospike.PutOp(aerospike.NewBin("Atime", TimeToDB(time.Now()))))
	if err != nil {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: Remove old entry: %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
		return syscall.EFAULT
	}
	// add req.NewName to req.NewDir(Ls)
	mp := aerospike.NewMapPolicy(aerospike.MapOrder.KEY_ORDERED, aerospike.MapWriteMode.CREATE_ONLY)
	lsVal := &LsItem{
		Inode: uint64(oinode),
		Type:  otype,
	}
	parentKey, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(nd.inode))
	if err != nil {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: NewKey(new): %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
		return syscall.EFAULT
	}
	_, err = d.fs.asd.Operate(mrt.Write(), parentKey, aerospike.MapPutOp(mp, "Ls", req.NewName, lsVal.ToAerospikeMap()), aerospike.PutOp(aerospike.NewBin("Mtime", TimeToDB(time.Now()))), aerospike.PutOp(aerospike.NewBin("Atime", TimeToDB(time.Now()))))
	if err != nil {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: Add new entry: %s", req.OldName, req.NewName, d.inode, req.NewDir, err)
		return syscall.EFAULT
	}
	// done
	xerr := mrt.Commit()
	if xerr != nil {
		mrt.Abort()
		log.Detail("Rename %s->%s on %d->%d: Commit: %s", req.OldName, req.NewName, d.inode, req.NewDir, xerr)
		return syscall.EFAULT
	}
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	nType, inode, err := d.lookup(ctx, name, GetWritePolicyNoMRT(d.fs.asd, &d.fs.cfg.Aerospike.Timeouts))
	if err != nil {
		return nil, err
	}
	switch nType {
	case fuse.DT_Dir:
		log.Detail("Lookup: Inode %d name %s: Dir inode %d", d.inode, name, inode)
		return &Dir{
			fs:    d.fs,
			inode: inode,
		}, nil
	case fuse.DT_File:
		log.Detail("Lookup: Inode %d name %s: File inode %d", d.inode, name, inode)
		return &File{
			fs:    d.fs,
			inode: inode,
		}, nil
	default:
		return nil, syscall.ENOTSUP
	}
}

func (d *Dir) lookup(ctx context.Context, name string, wp *aerospike.WritePolicy) (nType fuse.DirentType, inode uint64, err error) {
	// read the `Ls` entries, but do not return them, instead check if the entry with a given name exists
	log.Debug("Executing Lookup inode %d name %s", d.inode, name)
	k, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		log.Error("Lookup (%d,%s) NewKey: %s", d.inode, name, err)
		return 0, 0, syscall.EFAULT
	}
	r, err := d.fs.asd.Operate(wp, k, aerospike.MapGetByKeyOp("Ls", name, aerospike.MapReturnType.VALUE))
	if err != nil {
		log.Error("Lookup (%d,%s) Operate: %s", d.inode, name, err)
		return 0, 0, syscall.EFAULT
	}
	v := r.Bins["Ls"]
	if v == nil {
		log.Detail("Lookup: Inode %d name %s: ENOENT", d.inode, name)
		return 0, 0, syscall.ENOENT
	}
	return fuse.DirentType(v.(map[interface{}]interface{})["Type"].(int)), uint64(v.(map[interface{}]interface{})["Inode"].(int)), nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return d.readDirAll(ctx, GetReadPolicyNoMRT(d.fs.asd, &d.fs.cfg.Aerospike.Timeouts))
}

func (d *Dir) readDirAll(ctx context.Context, rp *aerospike.BasePolicy) ([]fuse.Dirent, error) {
	// read the `Ls` map entries and return them as a list of Dirent
	log.Debug("Executing ReadDirAll inode %d", d.inode)
	ret := []fuse.Dirent{}
	var err error
	k, err := aerospike.NewKey(d.fs.cfg.Aerospike.Namespace, "fs", int(d.inode))
	if err != nil {
		log.Error("ReadDirAll %d NewKey: %s", d.inode, err)
		return nil, syscall.EFAULT
	}
	r, xerr := d.fs.asd.Get(rp, k, "Ls")
	if xerr != nil {
		if xerr.Matches(aerospike.ErrKeyNotFound.ResultCode) {
			return nil, syscall.ENOENT
		}
		log.Error("ReadDirAll %d Get(Ls): %s", d.inode, xerr)
		return nil, syscall.EFAULT
	}
	log.Detail("ReadDirAll %d: Ls:%v", d.inode, r.Bins["Ls"])
	for n, v := range r.Bins["Ls"].(map[interface{}]interface{}) {
		ret = append(ret, fuse.Dirent{
			Inode: uint64(v.(map[interface{}]interface{})["Inode"].(int)),
			Name:  n.(string),
			Type:  fuse.DirentType(v.(map[interface{}]interface{})["Type"].(int)),
		})
	}
	return ret, nil
}
