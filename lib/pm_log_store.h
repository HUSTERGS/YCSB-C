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

    inline std::string GenerateRawEntry(std::string entry){
        uint64_t size = entry.size();
        std::string raw_size((char*)(&size), sizeof(uint64_t));
        //return std::string("user").append(zeros, '0').append(key_num_str);
        return raw_size + entry;
    }

    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)raw;
        return *size;
    }

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
            if (tail_.load(std::memory_order_release) + size >= mapped_len_) {
                printf("run out of log space\n");
                exit(-1);
            }
            return tail_.fetch_add(size, std::memory_order_relaxed);
        }
        void Append(PmAddr offset, const std::string& payload){
            pmem_memcpy_persist(raw_ + offset, payload.c_str(), payload.size());
        }

        char* raw() const {return raw_;}

    private:
        char* raw_;
        std::atomic<uint64_t> tail_;
        size_t mapped_len_;
        int is_pmem_;
    };
}

#endif //YCSB_PM_LOG_STORE_H
