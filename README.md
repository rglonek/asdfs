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

### Minimal Config file

```yaml
aerospike:
  host: 127.0.0.1
  port: 3000
  namespace: test
```

### Full Config file

```yaml
aerospike:
  host: 127.0.0.1
  port: 3000
  namespace: test
  timeouts:
    total: 120s
    socket: 30s
    mrt: 120s
    connect: 60s
    login: 60s
  auth:
    username: ""
    password: ""
    mode: "" # external / internal / pki
  tls:
    caFile: ""
    certFile: ""
    keyFile: ""
    tlsName: ""
fs:
  rootMode: 0o755
log:
  level: 6 # -1=NO_LOGGING 1=CRITICAL, 2=ERROR, 3=WARNING, 4=INFO, 5=DEBUG, 6=DETAIL
  kmesg: false
  file: ""
  stderr: true
```

### Client mount:

```
mount -t asdfs /etc/asdfs.yaml /test
```

## TODO

* we need locking and retires to handle multiple writes to the same directory and file
* EFAULT->EIO
* do we need to check permissions against user uid/gid and print EACCES ?
* MRT blocked->EBUSY
* EEXIST,ENOTDIR,EISDIR,EFBIG,ENOSPC,ETIMEDOUT,ENOTEMPTY
* Add a github workflow to make linux releases

## Wishlist

* multi-asd-record file storage - to allow for files larger than 8MiB
  * PK=(string)=inode_blockNo
  * will affect truncate
  * will affecrt truncate(stat)
  * will affect file read
  * will affect file write
  * will affect remove
  * store no more than 7MiB per block, leaving 1MiB for other bins, metadata, overheads, expansion, etc
  * implement menthods for partial reads (offset reads)
