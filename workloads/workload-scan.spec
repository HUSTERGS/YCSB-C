# Yahoo! Cloud System Benchmark
# Workload-read: Read only workload
#   Application example: Session store recording recent actions
#
#   Read/update ratio: 100/0
#   Default data size: 1 KB records (10 fields, 100 bytes each, plus key)
#   Request distribution: uniform

recordcount=10000
operationcount=10000
workload=com.yahoo.ycsb.workloads.CoreWorkload

readallfields=true

readproportion=0
updateproportion=0
scanproportion=1
insertproportion=0

requestdistribution=uniform