#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@
pushd . > /dev/null

cd /usr/share/zoneinfo
find * -type f -exec sh -c "diff -q /etc/localtime '{}' > /dev/null && echo {}" \;| awk '{print$1}'

popd > /dev/null
