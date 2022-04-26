# Yahoo! Cloud System Benchmark
# Workload-read: Read only workload
#   Application example: Session store recording recent actions
#
#   Delete ratio: 100
#   Default data size: 256 B records (8 fields, 32 bytes each, plus key)
#   Request distribution: uniform

recordcount=100000000
operationcount=10000000

#recordcount=160000
#operationcount=32000

fieldcount=1
fieldlength=144

#fieldcount=1
#fieldlength=8

workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=1
scanproportion=0
insertproportion=0

requestdistribution=uniform