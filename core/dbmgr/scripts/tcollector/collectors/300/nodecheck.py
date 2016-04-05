#!/usr/bin/python

import subprocess
import sys
import time
import re
import os
#from collectors.lib import utils

interval = 60  # seconds
cmd = "tradnodestatus"

def main():
    """sqnodecheck main loop"""
    
    #We only collect these metrics from one node in the cluster
    #If CMON is running, then collect and report metrics, else exit
    cmon_node = os.environ.get('CMON_RUNNING')
    if cmon_node != '1':
        sys.exit(-1)

    #utils.drop_privileges()
    try:
    	p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        ts = int(time.time())
        upNodes = 0
        downNodes = 0
        downNodesStr = []
        for line in p.stdout.readlines():
            if "UP" in line:
                upNodes = upNodes + 1	
            elif "DOWN" in line:
                downNodes = downNodes + 1
                downNodesStr.append(line.split(None, 1)[0])
        
        print("esgyn.nodes.up %d %d" % (ts, upNodes))
        if downNodes > 1:
            print("esgyn.nodes.down %d %d nodes=%s" % (ts, downNodes, ', '.join(downNodesStr)))
            
        sys.stdout.flush()
    except:
        print "Unexpected error:", sys.exc_info()[0]
 
if __name__ == "__main__":
    sys.exit(main())
