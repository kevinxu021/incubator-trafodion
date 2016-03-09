#!/usr/bin/env python
import sys
import subprocess
import logging
import os
import threading
import datetime

def registerMetric(logger, tsdbToolsDir):
        try:
                #print "command %s start at:%s" % (datetime.datetime.now())
                #Scan through the list of metrics in new_metrics.txt and register them
		file_obj=open(tsdbToolsDir + '/new_metrics.txt')
                register_metrics = file_obj.readlines()
		if len(register_metrics)==0:
			print "There are no new metrics to register!"
			logger.info("There are no new metrics to register!")
			return
                os.environ['CLASSPATH']=''
		threads = []
		metrics=[]
                for metric in register_metrics:
			metrics.append(metric)
			if len(metrics)%5==0:
				cmd='$MGBLTY_INSTALL_DIR/opentsdb/bin/tsdb mkmetric %60s %60s %60s %60s %60s' %(metrics[0].strip('\n'),metrics[1].strip('\n'),metrics[2].strip('\n'),metrics[3].strip('\n'),metrics[4].strip('\n'))
				cmd+=' --config=$MGBLTY_INSTALL_DIR/opentsdb/etc/opentsdb/opentsdb.conf 2>&1 &'
				metrics=[]
				th = threading.Thread(target=execCmd, args=(cmd,logger))
				threads.append(th)
		if len(metrics)!=0:
			for m in metrics:
				cmd='$MGBLTY_INSTALL_DIR/opentsdb/bin/tsdb mkmetric %60s --config=$MGBLTY_INSTALL_DIR/opentsdb/etc/opentsdb/opentsdb.conf 2>&1 &' %(m.strip('\n'))
				th = threading.Thread(target=execCmd, args=(cmd,logger))
				threads.append(th)

		#wait until thread ends
		for t in threads:
			t.start()
			while True:
				if(len(threading.enumerate()) < 6):
					break
		#print "command %s end at:%s" % (datetime.datetime.now())
        except Exception,e:
                logger.error("this is python error msg: %100s" % e)
        finally:
        	file_obj.close()
		#if len(register_metrics)==0:
                #        print("there is no metric need to register")
		#else:
		#	print("all the metrics are registered successfully!")
		
def execCmd(cmd,logger):
    try:
        p=subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for output in p.stdout.readlines():
        	print(output)
		logger.debug(output)
    except Exception, e:
    	print '%s\t fail to run,the cuase is:\r\n%s' % (cmd,e)
		
def computeListofMetricsToRegister(logger, tsdbToolsDir):

        try:
		#set enviroment variable
		file_obj=open(tsdbToolsDir + '/metrics.txt')
		HBASE_HOME = os.environ.get('HBASE_HOME')
		if HBASE_HOME == 'None':
                        os.environ['HBASE_HOME']='/usr/lib/hbase'

		#get all the list of metrics that are already registered into existing_metrics.txt
                step_f="echo 'scan \"tsdb-uid\", {COLUMNS => \"id:metrics\"}' | $HBASE_HOME/bin/hbase shell > tmp.txt"
                p=subprocess.Popen(step_f, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in p.stdout.readlines():
                        logger.debug(line)
                step_s="cat tmp.txt|awk '{print $1}' |awk '/[.]/' > " + tsdbToolsDir + "/existing_metrics.txt"
                p=subprocess.Popen(step_s, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in p.stdout.readlines():
                        logger.debug(line)

                #Read the list of required metrics from metrics.txt
		all_the_metrics = file_obj.readlines()
		allMetrics=[]

                #Compare the required metrics list with the already registered metrics list
		#and compute the list of metrics that are yet to be registered
		for i in all_the_metrics:
			allMetrics.append(i.strip())
		file_obj=open(tsdbToolsDir + '/existing_metrics.txt')
		existing_metrics=file_obj.readlines()
		existingMetrics=[]
		for i in existing_metrics:
			existingMetrics.append(i.strip())	
		register_metrics=list(set(allMetrics).difference(set(existingMetrics)))
		new_metrics=[]
		for i in register_metrics:
			if i.split():
				new_metrics.append(i+'\n')

                #Capture the list of metrics yet to be registered in new_metrics.txt
		file_obj=open(tsdbToolsDir + "/new_metrics.txt",'w')
                file_obj.writelines(new_metrics)
		p=subprocess.Popen("rm tmp.txt " + tsdbToolsDir + "/existing_metrics.txt", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		for line in p.stdout.readlines():
                	logger.debug(line)
        except Exception,e:
        	logger.debug('here is the error message: %100s'% e) 
	finally:
                file_obj.close()


if __name__=="__main__":
        TSDB_ROOT = os.environ.get('MGBLTY_INSTALL_DIR') + '/opentsdb'
        LOG_DIR = TSDB_ROOT + "/log"
        TOOLS_DIR = TSDB_ROOT + "/tools"
        
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)
	logger = logging.getLogger("loggingmodule.NomalLogger")
        handler = logging.FileHandler(LOG_DIR +"/registerMetrics.log")
        formatter = logging.Formatter("[%(levelname)s][%(funcName)s][%(asctime)s]%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

	computeListofMetricsToRegister(logger, TOOLS_DIR)
        registerMetric(logger, TOOLS_DIR)



