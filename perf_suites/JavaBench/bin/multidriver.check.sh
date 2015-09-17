pdsh $MY_NODES ps -ef | grep WorkloadDriver | grep -v grep | awk '{ printf "%s%d\n",$1,$3}' 
pdsh $MY_NODES ps -ef | grep WorkloadDriver | grep -v grep | awk '{ printf "%s%d\n",$1,$3}' | wc -l
