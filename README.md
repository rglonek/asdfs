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
log:
  level: 6 # 0=NO_LOGGING 1=CRITICAL, 2=ERROR, 3=WARNING, 4=INFO, 5=DEBUG, 6=DETAIL
  kmesg: false
  file: ""
  stderr: true
auth:
  username: ""
  password: ""
  mode: "" # external / internal / pki
tls:
  caFile: ""
  certFile: ""
  keyFile: ""
  tlsName: ""
```

### Client mount:
```
mount -t asdfs /etc/asdfs.yaml /test
```

## TODO

* custom filesystem and asd timeouts, using yaml configuration
* multi-asd-record file storage - to allow for files larger than 8MiB
* support symlinks
* support hardlinks (Nlink, plus linking in `ls` in dir entries)
* background the mount command instead of it running in the foreground - why are we not returning
* enable MRTs in asd.go: var MRTEnabled = true
