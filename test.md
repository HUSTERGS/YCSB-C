repo : `git clone xx`

submodule : 
`git submodule init`

`git submodule update --remote`

cmake:

`mkdir build && cd build`

`cmake .. -DCMAKE_BUILD_TYPE=Release`

`make`

docker environment:

`docker-compose build`

`docker-compose up`

get the container id by: `docker container ls`

connect docker by `docker exec -it xxxx /bin/bash`
or `ssh -p 45678 root@localhost`
*45678 is the port defined in the docker-compose.yml, you can change it to the port you prefer*

run ycsb: `numactl -N x ./ycsb -db hikv -threads 16 -P ../workloads/workload-read.spec -file_ratio 20`