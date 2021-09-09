./ycsb -db pmem-rocksdb -threads 32 -P ../workloads/workload-insert.spec -file_ratio 20
./ycsb -db pmem-rocksdb -threads 32 -P ../workloads/workload-read.spec -file_ratio 20
./ycsb -db pmem-rocksdb -threads 32 -P ../workloads/workload-delete.spec -file_ratio 20
./ycsb -db pmem-rocksdb -threads 32 -P ../workloads/workload-scan.spec -file_ratio 20