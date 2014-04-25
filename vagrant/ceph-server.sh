apt-get update
apt-get install -y ceph

cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid = b67bad36-3273-11e3-a2ed-0200000312bf
public network = 10.0.2.15/24
cluster network = 192.168.251.100/24
osd pool default crush rule = 0
osd pool default min size = 1
osd pool default size = 2
osd pool default pg num = 32
osd pool default pgp num = 32
mon host = ceph

debug auth = 0
debug filestore = 0
debug journal = 0
debug mon = 0
debug monc = 0
debug ms = 0
debug osd = 0
debug paxos = 0

[client]
log file = /var/log/ceph/client.log
rbd cache = true
rbd cache size = 67108864
rbd cache max dirty = 50331648
rbd cache target dirty = 33554432
rbd default format = 2

[mon]
mon addr = 192.168.251.100
mon data = /srv/ceph/mon/$cluster-$id
mon data avail crit = 2
mon data avail warn = 5
#
# ceph.conf-mon
# Managed by Puppet: do not edit this file directly. It will be overwritten!

[mon.ceph]
host = patty
mon addr = 10.0.2.15:6789
public addr = 10.0.2.15:6789
cluster addr = 192.168.251.100:6789
#
# ceph.conf-osd
# global osd configuration
# Managed by Puppet: do not edit this file directly. It will be overwritten!
[osd]
public addr = 10.0.2.15
cluster addr = 192.168.251.100
filestore fiemap = true
osd deep scrub interval = 2592000
osd journal size = 0
osd max backfills = 1
osd op complaint time = 15
osd recovery max active = 1
osd recovery op priority = 1
post start command = sleep 10
#
# ceph.conf-osd-vol
# OSD configuration for an individual volume.
# Managed by Puppet: do not edit this file directly. It will be overwritten!
[osd.1]
host = ceph
osd uuid = f673ce0b-25f3-5f26-adae-1702f3b8fd3f
osd data = /srv/ceph/osd.1
osd journal = /srv/ceph/osd.journal.1
filestore max sync interval = 102
EOF

ceph-osd -i 1 --mkfs --mkkey --mkjournal
ceph auth add "osd.1" osd 'allow *' mon 'allow rwx' -i "${dir}/keyring"
ceph osd crush add $name $weight root=default datacenter="$PUPPET_LOCATION" rack="${RACK// /_}" host="$HOSTNAME"

cat > /etc/ceph/ceph.keyring <<EOF
[client.kyle05]
    key = AQCwsgFTYBfAExAA+WHDVHX/dK1967s/D9bnyA==
    caps mon = "allow r"
    caps osd = "allow rwx"

[mon.]
    key = AQAX9FdSuPMeOhAApGUtT07MJ+tdZr+Zt1LisQ==
[client.admin]
    key = AQAX9FdSWNDuOBAAcsfOX7/GQ/SZ229FRsx2aQ==
    caps mds = "allow *"
    caps mon = "allow *"
    caps osd = "allow *"

[client.kyle09]
    key = AQBWrUNTsJFZABAA5IVnImvxJi9Yn8WaA41Xiw==
    caps mon = "allow r"
    caps osd = "allow rwx"

[client.kyle08]
    key = AQDntwFTKMd5ChAAWdj2t7KORgW2AkbabgzVBw==
    caps mon = "allow r"
    caps osd = "allow rwx"

[client.kyle07]
    key = AQAMtAFT4JsqMRAA+RS42UzsvD3Tw6AqCbqVeg==

/etc/init.d/ceph restart