repo : git clone xx

submodule : 
- git submodule init
- git submodule update --remote

cmake:

mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make

numactl -N x ./ycsb -db hikv -threads 16 -P ../workloads/workload-read.spec -file_ratio 20