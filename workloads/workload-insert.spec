# insert only
recordcount=100000000
operationcount=10000000
#recordcount=1600000
#operationcount=320000

fieldcount=1
fieldlength=144
#fieldcount=1
#fieldlength=8

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=0
scanproportion=0
insertproportion=1

requestdistribution=uniform

