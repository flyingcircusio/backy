#!/bin/bash

set -e
umask 022

HERE=$(cd $(dirname $0); pwd)
BACKUP=$(mktemp -d -t backy.test.XXXXX)
BACKY="${HERE}/bin/backy -l ${BACKUP}/backy.log"

mkdir ${BACKUP}/backup
cd ${BACKUP}/backup

# $BACKY init file ../original
cat > config <<EOF
---
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
$BACKY backup test
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
$BACKY backup test
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
$BACKY backup test
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
$BACKY backup test
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

# cleanup
if [ "$1" != "keep" ]; then
    rm -rf "$BACKUP"
else
    echo "Not deleting backup dir ${BACKUP}."
fi
