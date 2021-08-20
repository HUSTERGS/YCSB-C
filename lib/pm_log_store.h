//
// Created by zzyyyww on 2021/8/20.
//

#ifndef YCSB_PM_LOG_STORE_H
#define YCSB_PM_LOG_STORE_H

#include <libpmem.h>
#include <cstdint>
#include <string>
#include <atomic>

namespace pm {
    using PmAddr = uint64_t;
    class LogStore {
    public:
        explicit LogStore(const std::string& pm_path, uint64_t pm_size){
            raw_ = (char *) pmem_map_file(pm_path.c_str(), pm_size, PMEM_FILE_CREATE, 0666, &mapped_len_, &is_pmem_);
            if (raw_ == nullptr) {
                fprintf(stderr, "map file failed [%s]\n", strerror(errno));
                exit(-1);
            }
            tail_ = 0;
        }
        ~LogStore(){
            pmem_unmap(raw_, mapped_len_);
        }

        PmAddr Alloc(size_t size){
            return tail_.fetch_add(size, std::memory_order_relaxed);
        }
        void Append(PmAddr offset, cosnt std::string& payload){
            pmem_memcpy_persist(raw_ + offset, payload.c_str(), payload.size());
        }

    private:
        cosnt char* raw_;
        std::atomic<uint64_t> tail_;
        size_t mapped_len_;
        int is_pmem_;
    };
}

#endif //YCSB_PM_LOG_STORE_H
