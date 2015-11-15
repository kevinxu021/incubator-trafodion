#!/usr/bin/python

import subprocess
import sys
import time
import re
import os
#from collectors.lib import utils

interval = 60  # seconds
cmd = "hbcheck"

def main():
    """hbcheck main loop"""

    #We only collect these metrics from one node in the cluster
    #If CMON is running, then collect and report metrics, else exit
    cmon_node = os.environ.get('CMON_RUNNING')
    if cmon_node != '1':
        sys.exit(-1)

#    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#    utils.drop_privileges()
#    lines = open("input.txt", "r")
    try:
    	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ts = int(time.time())
        hcheck = None
        noNodes = 0
        for line in p.stdout.readlines():
            if "HBase is available!" in line:
                hcheck = True
            elif "Number of RegionServers available:" in line:
                noNodes = re.findall("\d+", line)
        
        if hcheck:
            print("esgyn.hbase.running %d 1" % ts)
            print("esgyn.hbase.regionserver %d %d" % (ts, int(noNodes[0])))
        else:
            print("esgyn.hbase.running %d 0" % ts)
            print("esgyn.hbase.regionserver %d 0" % ts)

		
        sys.stdout.flush()
    except:
        print "Unexpected error:", sys.exc_info()[0]

if __name__ == "__main__":
    sys.exit(main())
