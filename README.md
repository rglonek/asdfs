# Aerospike - asd mount

## Prerequisites

* FUSE3 - `apt update && apt -y install fuse3`
* Aerospike version 8.0.0.0+
* Aerospike strong consistency mode namespace

## Compile

```
go build -o /usr/sbin/mount.asdfs .
```

## Usage

### Config file

```yaml
aerospike:
  host: 127.0.0.1
  port: 3000
  namespace: test
fs:
  rootMode: 0o755
logLevel: 6 # 0=NO_LOGGING 1=CRITICAL, 2=ERROR, 3=WARNING, 4=INFO, 5=DEBUG, 6=DETAIL
```

### Client mount:
```
mount -t asdfs /etc/asdfs.yaml /test
```

## TODO

* support for tls, users in aerospike connection
* respect basic mount flags (rw/r, noatime, nomtime, etc)
* enable MRTs in asd.go: var MRTEnabled = true
* setAttr, uid, gid
* multi-asd-record file storage - to allow for files larger than 8MiB
* multi-asd-record dir Ls storage - to allow more files per directory
* custom filesystem and asd timeouts, using yaml configuration
* support symlinks
* support hardlinks (Nlink, plus linking in `ls` in dir entries)
* when returning with error from fuse calls, return syscall errors instead of raw messages
* support logging to /dev/kmesg
* inode cleanup/recycling?
* on writes and opens, update the Atime, Ctime, Mtime
* background the mount command instead of it running in the foreground - why are we not returning
* replace all `return err` with appropriate `syscall.E*` return codes
