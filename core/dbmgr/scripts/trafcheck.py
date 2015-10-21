#!/usr/bin/python

"""esgyndb stats for TSDB"""

import subprocess
import sys
import time
import re

#from collectors.lib import utils

interval = 30  # seconds
FIELDS = ("configure", "running", "down")
cmd = "sqcheck"

def main():
	"""sqcheck main loop"""
#       p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
#	utils.drop_privileges()

	try:
		p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		ts = int(time.time())
		for line in p.stdout.readlines():
			m = re.match("(DTM|RMS|MXOSRVR)(.*)", line)
			if not m:
				continue
			component = m.group(1).lower()
			stats = m.group(2).split(None)
			slen = len(stats)
			for i in xrange(slen):
				if (stats[i].isdigit):
					print("esgyndb.%s.%s %d %s" % (component, FIELDS[i], ts, stats[i]))
				else:
					print("esgyndb.%s.%s %d 0" % (component, FIELDS[i], ts))

			if slen == 2:
				print("esgyndb.%s.down %d 0" % (component, ts))

		sys.stdout.flush()
	except:
		print "Unexpected error:", sys.exc_info()[0]
if __name__ == "__main__":
    sys.exit(main())
