## EsgynDB Ansible Installer

### Install esgynDB on top of CDH5.4/HDP2.3/Vanilla Hadoop(work in progress)

**Prerequisite:**

 - Cloudera5.4.x/Hortonworks2.3 is installed on esgynDB nodes
  - CM or Ambari web UI is avaiable
 - /etc/hosts is configured correctly on installer's node
  - It contains all hostname info for esgynDB nodes
 - Ansible is installed on installer's node
  - epel repository is enabled
  - run command `` sudo yum install ansible `` to simply install ansbile 
 - Trafodion and esgynDB-manager RPM is stored on installer's node

**Features:**

 - Support installing on multiple clusters managed by one cloudera manager
 - Support running installer from any node, as long as it can ssh to esgynDB nodes
 - Support installing esgynDB on multiple regionserver groups in cloudera manager
 - Support installing esgynDB on non-passwordless ssh nodes
 - Auto discover for HBase regionserver nodes and hbase/hdfs system user


**Tips:**

- Ansible only needs to be installed on the node where you run the installer
- Don't need to input RPM file path if you put RPM files in the installer folder 
- Installation log is saved in the installer folder
- If the installation fails at ansible execution phase, next time you run installer, don't need to input parameters again, there will be a config file ``esgyndb_config`` located in the installer folder, installer will pick it up as user config.
- If the installation completes successfully, the config file will be renamed to ``esgyndb_config_bak.yymmdd_hhmm``, it can be used next time using ``--config-file`` option.

**Examples:**

- Install esgynDB in guide mode without esgynDB manager:

``esgyndb_install.py --no-dbmgr``

- Install esgynDB in config mode with 10 parallel processes (default is 5):

``esgyndb_install.py --config-file xxx_config --fork 10``

- Run installer from local system with user a, install esgynDB on remote nodes with user b, and user b has configured passwordless ssh:

`` esgyndb_install.py --remote-user b --disable-pass``

- Generate the config file only but not doing the real install:

`` esgyndb_install.py --dryrun``