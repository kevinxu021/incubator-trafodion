#!/bin/sh
exec 2>&1
exec ./scollector-linux-amd64 -c="../conf/scollector.toml"
