dbname="hybridhash"
types=(
  "-insert"
  "-read"
  "-delete"
  "-scan"
  "a"
  "b"
  "c"
  "d"
  "e"
  "f"
)
rm -f ycsbc.output
for tn in 32
do
    for type in ${types[@]} ;
    do
        for ratio in 2000
        do
        rm -f /mnt/pmem/metakv/*
        echo "Running $dbname workload$type with $tn threads && ratio $ratio " >> "ycsbc.output"
        cmd="numactl -N 0 ./ycsb -db $dbname -threads $tn -P ../workloads/workload$type.spec -file_ratio $ratio >> "ycsbc.output" "
        # echo $cmd >>ycsbc.output
        eval $cmd
        done
    done
done
