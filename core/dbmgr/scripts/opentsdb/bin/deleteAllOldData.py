#!/usr/bin/env python
import sys
import time
import	subprocess 
import logging
import os

def deleteAllOldData():
		
        try:
		TSDB_ROOT = os.environ.get('MGBLTY_INSTALL_DIR') + '/opentsdb'
        	LOG_DIR = TSDB_ROOT + "/log"
	        TOOLS_DIR = TSDB_ROOT + "/tools"
		if not os.path.exists(LOG_DIR):
            		os.makedirs(LOG_DIR)
		logger = logging.getLogger("loggingmodule.NomalLogger")
		time_str=time.strftime("%Y%m%d-%H%M%S")
		log_name='/deleteAllOldData'+time_str+'.log'
		handler = logging.FileHandler(LOG_DIR + log_name)
		formatter = logging.Formatter("[%(levelname)s][%(funcName)s][%(asctime)s]%(message)s")
		handler.setFormatter(formatter)
		logger.addHandler(handler)
		logger.setLevel(logging.DEBUG)
		file_obj=open(TOOLS_DIR+'/metrics.txt')
		start_time=sys.argv[1]
		end_time=sys.argv[2]
		all_the_metrics = file_obj.readlines()
		for metric in all_the_metrics:
            		cmd='$MGBLTY_INSTALL_DIR/opentsdb/bin/tsdb scan --delete %20s %20s sum %60s 2>&1 &' %(start_time,end_time,metric.strip('/n'))
			print(cmd)
			p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			for line in p.stdout.readlines():
				logger.debug(line)
			time.sleep(60)
	except OSError,e1:
		logger.error("this is an OSError msg! %100s" % e1)
	finally:
        	file_obj.close()
		print("The command completed. Please check the log "+log_name+" for details")
if __name__=="__main__":
	deleteAllOldData()

