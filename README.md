# Aerospike - asd mount

## Prerequisites

FUSE3 - `apt update && apt -y install fuse3`

## Compile

```
go build -o /usr/sbin/mount.asdfs .
```

## Usage

Client mount 1:
```
mount -t asdfs host=172.17.0.2,port=3100,ns=test,offset=1000000000 /test
```

Client mount 2:
```
mount -t asdfs host=172.17.0.2,port=3100,ns=test,offset=2000000000 /test
```

Offset ensures that each client mount offsets new inodes (no clashes between different clients mounting the same filesystem at once).

The rest of the parameters are the aerospike seed host, port and namespace.

Final parameter in mount is the destination folder to mount to.

## TODO

* chmod
* chown
* support for tls, users in aerospike connection
* support for updating mtime, atime on files and folders
* respect basic mount flags (rw/r, noatime, nomtime, etc)
* when available: use MRTs to ensure data doesn't get corrupted (so we won't need to code fsck)
  * at the moment, if a file is removed or created, and something happens in the process, we may end up with a runaway inode entry (a file that has an inode but is not referenced anywhere - essentially a forever-orphan)
  * this would be easy to cleanup as fsck, instead of using MRTs
* probably a lot more, but the basic implementation already works, so I am happy; code could do with cleanup, this was a quickly hacked PoC
