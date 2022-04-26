dbname="pmem-rocksdb"
types=(
  "-insert"
  "-read"
  "-delete"
  "-scan"
#  "a"
#  "b"
#  "c"
#  "d"
#  "e"
#  "f"
)
rm -f ycsbc-$dbname.output
rm -f ycsbc-$dbname-compress.output

for tn in 16 8 4 2 1
do
    for type in ${types[@]} ;
    do
        for ratio in 200
        do
#        rm -f /mnt/pmem01/metakv*
        rm -rf /mnt/pmem01/* 2> /dev/null
        rm -f /mnt/pmem01/.* 2> /dev/null
        echo "Running $dbname workload$type with $tn threads && ratio $ratio " >> "ycsbc-$dbname.output"
        cmd="sudo ./ycsb -db $dbname -threads $tn -P ../workloads/workload$type.spec -file_ratio $ratio >> "ycsbc-$dbname.output" "
        # echo $cmd >>ycsbc.output
        eval $cmd

#        rm -f /mnt/pmem01/metakv*
#                rm -rf /mnt/pmem01/* 2> /dev/null
        #        rm -f /mnt/pmem01/.* 2> /dev/null
#        echo "Running $dbname-compress workload$type with $tn threads && ratio $ratio " >> "ycsbc-$dbname-compress.output"
#        cmd="sudo ./ycsb-$dbname-compress -db $dbname -threads $tn -P ../workloads/workload$type.spec -file_ratio $ratio >> "ycsbc-$dbname-compress.output" "
        # echo $cmd >>ycsbc.output
#        eval $cmd

        done
    done
done
