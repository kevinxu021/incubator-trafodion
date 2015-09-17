#!/bin/bash
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corportation
#
# @@@ END COPYRIGHT @@@
cd /usr/share/zoneinfo
find * -type f -exec sh -c "diff -q /etc/localtime '{}' > /dev/null && echo {}" \; | awk 'NR==1{print$1}'
