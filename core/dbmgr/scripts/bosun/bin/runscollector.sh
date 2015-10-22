#!/bin/sh
exec 2>&1
exec ./scollector -c="../conf/scollector.toml"
