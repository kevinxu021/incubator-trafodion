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
 
#from collectors.lib import utils
 
cmd = "dtmci stats -j"
 
def main():
    """sqcheck main loop"""
 
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#    utils.drop_privileges()
 
    try:
        ts = int(time.time())
        json_data = p.stdout.readline()
        data = json.loads(json_data)
        ta = 0
        tb = 0
        tc = 0
        for i in xrange(len(data)):
            ta = ta + int(data[i]['txnStats']['txnAborts'])
            tb = tb + int(data[i]['txnStats']['txnBegins'])
            tc = tc + int(data[i]['txnStats']['txnCommits'])
        print ("esgyndb.dtm.txnaborts %d %d" % (ts, ta))
        print ("esgyndb.dtm.txnbegins %d %d" % (ts, tb))
        print ("esgyndb.dtm.txncommits %d %d" % (ts, tc))

        sys.stdout.flush()
    except:
        print "Unexpected error:", sys.exc_info()[0]
 
if __name__ == "__main__":
    sys.exit(main())

