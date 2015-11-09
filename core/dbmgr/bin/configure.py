#!/usr/bin/env python
# @@@ START COPYRIGHT @@@
#
# (C) Copyright 2015 Esgyn Corporation
#
# @@@ END COPYRIGHT 

from optparse import OptionParser
import getpass
import sys
import os
import io
import re
import fnmatch
import textwrap
import socket
import commands
import subprocess

from subprocess import Popen, PIPE
from sys import stderr

def cmd_exists(cmd):
    return subprocess.call("type " + cmd, shell=True, 
        stdout=subprocess.PIPE, stderr=subprocess.PIPE) == 0

def findfile(librarypath, pattern):
    ''' Locate the indicated file starting at the path given '''

    for root, dirs, files in os.walk(librarypath):
        for filename in fnmatch.filter(files, pattern):
            return(os.path.join(root, filename))
        

def cmd_output(cmd, title = '', isOneLine=False):
    '''
    Execute the indicated command and check the resulting status.  
    @argument cmd: the command to be executed    
    @argument title: the text logged (with the error) when the command fails.
    @argument isOneLine: set to True when all output should be returned as a single string, otherwise returned as a list of lines
    Note that the "find" command returns with an error if it encounters any 
    permission problems along the way.  Here we ignore that error.
    '''
    result =  [ ]
    pipe = Popen(cmd, stdout=PIPE, stderr=PIPE, shell=True)
    out, err = pipe.communicate( )
    if ((pipe.poll() == 0) or (out)):  # if any output is available, return it
        result = out.splitlines( )
    else:
        if (not title): title = 'command "' + cmd + '"'
        else:
            sys.stderr.write("%s failed with error %s\n"% (title, pipe.returncode))
                  

    return (result)

def encode_pswd(plain_text,dbmgr_path):
    ''' Encode the indicated plain text using Jetty's OBF obfuscation technique '''

    encoded = plain_text
    # Get the path of the jetty-util*.jar, which will be used in password obfucation command 

    librarypath = os.path.join(dbmgr_path, 'lib')
    jettysecurelib = findfile(librarypath, 'jetty-util*.jar')
    cmd = "java -cp %s org.eclipse.jetty.util.security.Password %s 2>&1" %(jettysecurelib, plain_text)
    output = cmd_output(cmd, "Encoding password using %s" %(jettysecurelib))
    for line in output:
        matchObj = re.match(r'OBF:(\w+)', line, re.M|re.I)
        if (matchObj): 
            encoded = line.strip( )
            break

    return (encoded)

def run():
    java_exists = cmd_exists('java')
    if not java_exists: 
        print 'Cannot find the java executable in PATH. Aborting operation. Please add java to PATH and try again.'
        return
    
    # step 1: Get the arguments from the user and validate it.
    time_zone = commands.getoutput("./gettimezone.sh | awk '{print $1}'| head -1")
    host_fqdnh = socket.gethostbyname(socket.getfqdn())
    parser = OptionParser()
    parser.add_option("--javahome",  dest="java_home",    action="store",
            help="Java Home path for keytool",    metavar="JAVA_HOME")
    parser.add_option("--httpport",  dest="http_port",   action="store", type="int",                  
            help="HTTP Port for the EsgynDB Manager",    metavar="HTTP_PORT")
    parser.add_option("--httpsport", dest="https_port",  action="store", type="int",                   
            help="HTTPS Port for the EsgynDB Manager",   metavar="HTTPS_PORT")    
    parser.add_option("--password",  dest="password",    action="store",                    
            help="The password for the SSL keystore",    metavar="PASSWORD")
    parser.add_option("--dcshost",   dest="dcs_host",    action="store",                    
            help="Hostname for the EsgynDB DCS server",  metavar="DCS_HOST")
    parser.add_option("--dcsport",   dest="dcs_port",    action="store", type="int",                  
            help="Port for the EsgynDB DCS server",      metavar="DCS_PORT")
    parser.add_option("--dcsinfoport",   dest="dcs_info_port",    action="store", type="int",                  
            help="Info Port for the EsgynDB DCS Master",      metavar="DCS_INFO_PORT")
    parser.add_option("--resthost",  dest="rest_host",   action="store",                    
            help="Hostname for the EsgynDB REST server", metavar="REST_HOST")
    parser.add_option("--restport",  dest="rest_port",   action="store", type="int",                     
            help="Port for the EsgynDB REST server",     metavar="REST_PORT")
    parser.add_option("--tsdhost",  dest="tsd_host",   action="store",                    
            help="Hostname for the OpenTSDB TSD", metavar="TSD_HOST")
    parser.add_option("--tsdport",  dest="tsd_port",   action="store", type="int",                     
            help="Port for the OpenTSDB TSD",     metavar="TSD_PORT")            
    parser.add_option("--timezone",  dest="time_zone",   action="store",                     
            help="Local TimeZone of the EsgynDB instance (Format like America/Los_Angeles or Etc/Utc)",     metavar="TIMEZONE_NAME")
    parser.add_option("--bosunhost",  dest="bosun_host",   action="store",                    
            help="Hostname for the Bosun server", metavar="BOSUN_HOST")
    parser.add_option("--bosunport",  dest="bosun_port",   action="store", type="int",                     
            help="Port for the Bosun server",     metavar="BOSUN_PORT")            

    (options, args) = parser.parse_args()
    
    # Now ensure all the arguments has an input value
    env = os.getenv("JAVA_HOME")
    if env is not None:
        options.java_home = env
    else:
        done = None
        while(done==None) :
            options.java_home = raw_input("Please provide the JAVA_HOME path for keytool:")
            if options.java_home: done = True
            
    if not options.http_port :
        done = None
        while (done==None) :
            options.http_port = raw_input("Please provide the HTTP port for EsgynDB Manager (default 4205): ")
            if not options.http_port : options.http_port = "4205"
            if options.http_port and options.http_port.isdigit(): done = True
            
    if not options.https_port :
        done = None
        while (done==None) :
            options.https_port = raw_input("Please provide the HTTPS port for EsgynDB Manager (default 4206): ")
            if not options.https_port : options.https_port = "4206"
            if options.https_port and options.https_port.isdigit(): done = True    
            
    if not options.password :
        done = None
        while (done==None) :
            options.password = getpass.getpass(prompt='Please provide the password for the SSL keystore (minimum 6 chars): ')
            #options.password = raw_input("Please provide the password for the SSL keystore (minimum 6 chars):")
            if options.password and len(options.password)>5: done = True
            
    if not options.dcs_host :
        done = None
        while (done==None) :
            options.dcs_host = raw_input("Please provide the EsgynDB DCS master hostname (default "+ host_fqdnh + "): ")
            if not options.dcs_host: options.dcs_host = host_fqdnh
            if options.dcs_host: done = True
            
    if not options.dcs_port :
        done = None
        while (done==None) :
            options.dcs_port = raw_input("Please provide the EsgynDB DCS master listen port (default 23400): ")
            if not options.dcs_port : options.dcs_port = "23400"
            if options.dcs_port and options.dcs_port.isdigit(): done = True
            
    if not options.dcs_info_port :
        done = None
        while (done==None) :
            options.dcs_info_port = raw_input("Please provide the EsgynDB DCS master info port (default 24400): ")
            if not options.dcs_info_port : options.dcs_info_port = "24400"
            if options.dcs_info_port and options.dcs_info_port.isdigit(): done = True
            
    if not options.rest_host :
        done = None
        while (done==None) :
            options.rest_host = raw_input("Please provide the EsgynDB REST server hostname (default "+ host_fqdnh + "): ")
            if not options.rest_host: options.rest_host = host_fqdnh
            if options.rest_host: done = True
            
    if not options.rest_port :
        done = None
        while (done==None) :
            options.rest_port = raw_input("Please provide the EsgynDB REST server port (default 4200): ")
            if not options.rest_port : options.rest_port = "4200"
            if options.rest_port and options.rest_port.isdigit(): done = True
            
    if not options.tsd_host :
        done = None
        while (done==None) :
            options.tsd_host = raw_input("Please provide the TSD hostname (default "+ host_fqdnh + "): ")
            if not options.tsd_host: options.tsd_host = host_fqdnh
            if options.tsd_host: done = True
            
    if not options.tsd_port :
        done = None
        while (done==None) :
            options.tsd_port = raw_input("Please provide the TSD port(default 5242): ")
            if not options.tsd_port : options.tsd_port = "5242"
            if options.tsd_port and options.tsd_port.isdigit(): done = True
            
    if not options.bosun_host :
        done = None
        while (done==None) :
            options.bosun_host = raw_input("Please provide the Bosun hostname (default "+ host_fqdnh + "): ")
            if not options.bosun_host: options.bosun_host = host_fqdnh
            if options.bosun_host: done = True
            
    if not options.bosun_port :
        done = None
        while (done==None) :
            options.bosun_port = raw_input("Please provide the Bosun port(default 8070): ")
            if not options.bosun_port : options.bosun_port = "8070"
            if options.bosun_port and options.bosun_port.isdigit(): done = True

    if not options.time_zone :
        done = None
        while (done==None) :
            options.time_zone = raw_input("Please provide the local TimeZone of the EsgynDB instance(default "+ time_zone + "): ")
            if not options.time_zone : options.time_zone = time_zone
            if options.time_zone: done = True
  
    # sqroot_path = os.getenv("MY_SQROOT")
    # if not sqroot_path: sqroot_path = os.getcwd()
    # dbmgr_path = os.path.join(sqroot_path,"dbmgr")
    #file_path = findfile('/','dbmgr_installer.py')
    #dbmgr_path = os.path.dirname(os.path.dirname(file_path))
    bin_path = os.path.dirname(os.path.realpath(__file__))
    dbmgr_path = os.path.abspath(os.path.join(bin_path, os.pardir))

    
    # Step 2: Generate the self-signed SSL keystore for the Jetty server
    print "Generating self-signed certificate and SSL keystore..."
    keyfile = os.path.join(dbmgr_path,"etc/dbmgr.keystore")
    if (os.path.exists(keyfile)): os.remove(keyfile)
    cmd = 'keytool -genkey -alias dbmgr -keyalg RSA -dname "cn='+host_fqdnh+',ou=dbmgr,o=Esgyn,l=Milpitas,st=CA,c=US" -keypass "%s" -storepass "%s" -keystore %s -validity 36500 -keysize 2048' % (options.password,options.password,keyfile)
    cmd = options.java_home+'/bin/'+cmd
    returncode = os.system(cmd)
    if (returncode != 0):
        print "Failed to generate the SSL keystore, exit..."
        sys.exit(1)
        
    # Step 3: password obfuscation
    obfuscated_pw = encode_pswd(options.password,dbmgr_path)

    
    # Step 4: configure the config.xml file with user inputs
    print "Creating DB Manager config file..."
    config_template = os.path.join(dbmgr_path,"conf/config_template.xml")
    with open(config_template) as f:
        file_str=f.read()
        
    file_str=file_str.replace('SECURE_PASSWORD', str(obfuscated_pw))
    file_str=file_str.replace('DCS_HOST', str(options.dcs_host))
    file_str=file_str.replace('DCS_PORT', str(options.dcs_port))
    file_str=file_str.replace('DCS_INFO_PORT', str(options.dcs_info_port))
    file_str=file_str.replace('REST_HOST', str(options.rest_host))
    file_str=file_str.replace('REST_PORT', str(options.rest_port))
    file_str=file_str.replace('HTTP_PORT', str(options.http_port))
    file_str=file_str.replace('HTTPS_PORT', str(options.https_port))
    file_str=file_str.replace('TIMEZONE_NAME', str(options.time_zone))
    file_str=file_str.replace('TSD_HOST', str(options.tsd_host))
    file_str=file_str.replace('TSD_PORT', str(options.tsd_port))
    file_str=file_str.replace('BOSUN_HOST', str(options.bosun_host))
    file_str=file_str.replace('BOSUN_PORT', str(options.bosun_port))

    config_xml = os.path.join(dbmgr_path,"conf/config.xml")
    with open(config_xml, 'w+') as f:
        f.write(file_str)
    
    # All work done
    print "EsgynDB Manager installation completed successfully."

##
## main code:
##
run()   
