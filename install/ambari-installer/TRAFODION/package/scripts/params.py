#!/usr/bin/env python
from resource_management import *

# config object that holds the configurations declared in the config xml file
config = Script.get_config()

dcs_servers = config['configurations']['trafodion-config']['dcs.servers']
