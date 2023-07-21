#!/usr/bin/env bash

set -e
umask 022

if [[ -z "$BACKY_CMD" ]]; then
  echo "error: BACKY_CMD is not set. Set it manually or call via pytest"
  exit 2
fi

BACKUP=$(mktemp -d -t backy.test.XXXXX)
BACKY="$BACKY_CMD -l ${BACKUP}/backy.log"
export TZ=Europe/Berlin

mkdir ${BACKUP}/backup
cd ${BACKUP}/backup

# $BACKY init file ../original
cat > config <<EOF
---
    schedule:
        daily:
            interval: 1d
            keep: 7
    source:
        type: file
        filename: ../original
EOF

echo "Using ${BACKUP} as workspace."

echo -n "Generating Test Data"
dd if=/dev/urandom of=$BACKUP/img_state1.img bs=104856 count=5 2>/dev/null
echo -n "."
dd if=/dev/urandom of=$BACKUP/img_state2.img bs=104856 count=5 2>/dev/null
echo -n "."
dd if=/dev/urandom of=$BACKUP/img_state3.img bs=104856 count=5 2>/dev/null
echo " Done."

echo -n "Backing up img_state1.img. "
ln -sf img_state1.img ../original
$BACKY backup manual:test
echo "Done."

echo -n "Backing up img_state1.img with unknown tag. "
$BACKY backup unknown && exit 1
echo "Done."

echo -n "Restoring img_state1.img from level 0. "
$BACKY restore -r 0 ../restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$(diff $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

# cleanup
rm $BACKUP/restore_*


echo -n "Backing up img_state2.img. "
ln -sf img_state2.img ../original
$BACKY backup daily
echo "Done."

echo -n "Restoring img_state2.img from level 0. "
$BACKY restore -r 0 ../restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$(diff $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state1.img from level 1. "
$BACKY restore -r 1 ../restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$(diff $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

# cleanup
rm $BACKUP/restore_*



echo -n "Backing up img_state2.img again. "
ln -sf img_state2.img ../original
$BACKY backup -f test
echo "Done."

echo -n "Restoring img_state2.img from level 0. "
$BACKY restore -r 0 ../restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$(diff $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

rm $BACKUP/restore_state2.img

echo -n "Restoring img_state2.img from level 1. "
$BACKY restore -r 1 ../restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$(diff $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state1.img from level 2. "
$BACKY restore -r 2 ../restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$(diff $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

# cleanup
rm $BACKUP/restore_*


echo -n "Backing up img_state3.img. "
ln -sf img_state3.img ../original
$BACKY backup manual:test
echo "Done."

echo -n "Restoring img_state3.img from level 0. "
$BACKY restore -r 0 ../restore_state3.img
echo "Done."

echo -n "Diffing restore_state3.img against img_state3.img. "
res=$(diff $BACKUP/restore_state3.img $BACKUP/img_state3.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state2.img from level 1. "
$BACKY restore -r 1 ../restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$(diff $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

rm $BACKUP/restore_state2.img

echo -n "Restoring img_state2.img from level 2. "
$BACKY restore -r 2 ../restore_state2.img
echo "Done."

echo -n "Diffing restore_state2.img against img_state2.img. "
res=$(diff $BACKUP/restore_state2.img $BACKUP/img_state2.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

echo -n "Restoring img_state1.img from level 3. "
$BACKY restore -r 3 ../restore_state1.img
echo "Done."

echo -n "Diffing restore_state1.img against img_state1.img. "
res=$(diff $BACKUP/restore_state1.img $BACKUP/img_state1.img)
if [ "" == "$res" ]; then
    echo "Success."
else
    echo "ERROR: $res"
    exit 2
fi

$BACKY status

$BACKY forget -r 2

$BACKY status

# cleanup
if [ "$1" != "keep" ]; then
    rm -rf "$BACKUP"
else
    echo "Not deleting backup dir ${BACKUP}."
fi
