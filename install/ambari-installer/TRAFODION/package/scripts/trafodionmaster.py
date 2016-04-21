import sys, os, pwd, signal, time
from resource_management import *
from subprocess import call
from shutil import copyfile

class Master(Script):
  def install(self, env):
  
    # Install packages listed in metainfo.xml
    self.install_packages(env)
    self.configure(env)
    import params
  
    dir = os.path.realpath(__file__).split('/scripts')[0]

    self.configure(env)

    dcs = str(params.dcs_servers)

    f = open( '/etc/trafodion/trafodion_config', 'a')
    f.write ( 'export DCS_SERVERS_PARM="' + dcs + '"\n' )
    f.close()

    copyfile('/etc/trafodion/trafodion_config', '/tmp/temp_config') 
    returnCode = os.system("bash -c \"sudo /tmp/trafscripts/trafodion_master_install --config_file /tmp/temp_config\"")

    if returnCode == 0:
       print "Trafodion Master Installed!"
    else:
       sys.exit(-1) 

    self.configure(env)

    #if any other install steps were needed they can be added here

  def configure(self, env):
    import params
    env.set_params(params)

  #To stop the service, use the linux service stop command and pipe output to log file
  def stop(self, env):
    import params 
    Execute('sudo su trafodion --login --command "sqstop"')

  #To start the service, use the linux service start command and pipe output to log file      
  def start(self, env):
    import params
    Execute('sudo su trafodion --login --command "sqstart"')
	
  #To get status of the, use the linux service status command      
  def status(self, env):
    import params
    Execute('sudo su trafodion --login --command "sqcheck"')
 
  def initialize(self, env):
    import params
    returnCode = os.system("bash -c \"sudo /tmp/trafscripts/initializeTrafodion\"")

    if returnCode == 0:
       print "good!"
    else:
       sys.exit(-1)
 

if __name__ == "__main__":
  Master().execute()
