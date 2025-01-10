# make build env image
# docker build -t gofuse .
# run build
docker run -it --rm -v ./:/mnt/ gofuse bash -c "cd /mnt && /root/go/bin/go build -o mount.asdfs ."
# run test
aerolab cluster create -n asdfs -o aerospike.conf -v 8.0.0.0-rc1 --privileged --no-autoexpose
aerolab files upload -n asdfs mount.asdfs /usr/sbin/mount.asdfs
aerolab files upload -n asdfs asdfs.yaml /etc/asdfs.yaml
aerolab attach shell -n asdfs -- bash -c "apt update && apt -y install fuse3"
aerolab attach shell -n asdfs -- mkdir /test
aerolab roster apply -n asdfs -m test
aerolab attach shell -n asdfs --detach -- mount -t asdfs /etc/asdfs.yaml /test
while true; do
    echo "Press ENTER to rededploy"
    read
    aerolab attach shell -n asdfs -- umount /test
    docker run -it --rm -v ./:/mnt/ gofuse bash -c "cd /mnt && /root/go/bin/go build -o mount.asdfs ."
    aerolab files upload -n asdfs mount.asdfs /usr/sbin/mount.asdfs
    aerolab attach shell -n asdfs --detach -- mount -t asdfs /etc/asdfs.yaml /test
done
# aerolab cluster destroy -f -n asdfs
# mount -t asdfs /etc/asdfs.yaml /test
