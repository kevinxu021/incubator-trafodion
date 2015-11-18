#!/usr/bin/python
 
################################################################################
#
#             EsgynDB Transactions 
#             This script publishes list of transactions on the system
#             
################################################################################
 
import subprocess
import sys
import time
import re
import json
import os 
#from collectors.lib import utils
 
cmd = "dtmci stats -j"
 
def main():
    """dtmstats main loop"""
    
    #We only collect these metrics from one node in the cluster
    #If CMON is running, then collect and report metrics, else exit
    cmon_node = os.environ.get('CMON_RUNNING')
    if cmon_node != '1':
        sys.exit(-1)

    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#    utils.drop_privileges()
 
    try:
        ts = int(time.time())
        json_data = ""
        for line in p.stdout.readlines():
            json_data = json_data + line
        data = json.loads(json_data)
        ta = 0
        tb = 0
        tc = 0
        for i in xrange(len(data)):
            ta = int(data[i]['txnStats']['txnAborts'])
            tb = int(data[i]['txnStats']['txnBegins'])
            tc = int(data[i]['txnStats']['txnCommits'])
            print ("esgyndb.dtm.txnaborts %d %d node=%d" % (ts, ta, i))
            print ("esgyndb.dtm.txnbegins %d %d node=%d" % (ts, tb, i))
            print ("esgyndb.dtm.txncommits %d %d node=%d" % (ts, tc, i))

        sys.stdout.flush()
    except:
        print "Unexpected error:", sys.exc_info()[0]
 
if __name__ == "__main__":
    sys.exit(main())

