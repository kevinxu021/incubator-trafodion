#!/usr/bin/env python

from optparse import OptionParser
import getpass
import sys
import os
import io
import re
import fnmatch
import textwrap
from subprocess import Popen, PIPE
from sys import stderr


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
    
    # step 1: Get the arguments from the user and validate it.
    parser = OptionParser()
    
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
            help="Info Port for the EsgynDB DCS Master",      metavar="DCS_PORT")
    parser.add_option("--resthost",  dest="rest_host",   action="store",                    
            help="Hostname for the EsgynDB REST server", metavar="REST_HOST")
    parser.add_option("--restport",  dest="rest_port",   action="store", type="int",                     
            help="Port for the EsgynDB REST server",     metavar="REST_PORT")
    parser.add_option("--timezone",  dest="time_zone",   action="store",                     
            help="Local TimeZone of the EsgynDB instance (Format like America/Los_Angeles or Etc/Utc)",     metavar="TIMEZONE_NAME")

    (options, args) = parser.parse_args()
    
    # Now ensure all the arguments has an input value
    if not options.http_port :
        done = None
        while (done==None) :
            options.http_port = raw_input("Please provide the HTTP port for EsgynDB Manager (default 4205):")
            if not options.http_port : options.http_port = "4205"
            if options.http_port and options.http_port.isdigit(): done = True
            
    if not options.https_port :
        done = None
        while (done==None) :
            options.https_port = raw_input("Please provide the HTTPS port for EsgynDB Manager (default 4206):")
            if not options.https_port : options.https_port = "4206"
            if options.https_port and options.https_port.isdigit(): done = True    
            
    if not options.password :
        done = None
        while (done==None) :
            options.password = getpass.getpass(prompt='Getpass Please provide the password for the SSL keystore (minimum 6 chars):')
            #options.password = raw_input("Please provide the password for the SSL keystore (minimum 6 chars):")
            if options.password and len(options.password)>5: done = True
            
    if not options.dcs_host :
        done = None
        while (done==None) :
            options.dcs_host = raw_input("Please provide the EsgynDB DCS server hostname:")
            if options.dcs_host: done = True
            
    if not options.dcs_port :
        done = None
        while (done==None) :
            options.dcs_port = raw_input("Please provide the EsgynDB DCS server port(default 23400):")
            if not options.dcs_port : options.dcs_port = "23400"
            if options.dcs_port and options.dcs_port.isdigit(): done = True
            
    if not options.dcs_info_port :
        done = None
        while (done==None) :
            options.dcs_info_port = raw_input("Please provide the EsgynDB DCS master info port(default 24400):")
            if not options.dcs_info_port : options.dcs_info_port = "24400"
            if options.dcs_info_port and options.dcs_info_port.isdigit(): done = True
            
    if not options.rest_host :
        done = None
        while (done==None) :
            options.rest_host = raw_input("Please provide the EsgynDB REST server hostname:")
            if options.rest_host: done = True
            
    if not options.rest_port :
        done = None
        while (done==None) :
            options.rest_port = raw_input("Please provide the EsgynDB REST server port(default 4200):")
            if not options.rest_port : options.rest_port = "4200"
            if options.rest_port and options.rest_port.isdigit(): done = True
            
    if not options.time_zone :
        done = None
        while (done==None) :
            options.time_zone = raw_input("Please provide the local TimeZone of the EsgynDB instance(default Etc/Utc):")
            if not options.time_zone : options.time_zone = "Etc/UTC"
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
    cmd = 'keytool -genkey -alias dbmgr -keyalg RSA -dname "cn=localhost,ou=dbmgr,o=Esgyn,l=Milpitas,st=CA,c=US" -keypass "%s" -storepass "%s" -keystore %s -validity 36500 -keysize 2048' % (options.password,options.password,keyfile)
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

    config_xml = os.path.join(dbmgr_path,"conf/config.xml")
    with open(config_xml, 'w+') as f:
        f.write(file_str)
    
    # All work done
    print "EsgynDB Manager installation completed successfully."

##
## main code:
##
run()   
