//
// Created by zzyyyww on 2021/8/23.
//

#include <unistd.h>
#include <cstdlib>
#include <stdint.h>
#include <iostream>

#include "cceh_db.h"
#include "global_log.h"
#include "core/timer.h"

namespace cceh_db {

#define CPU_FREQ_MHZ (1994)  // cat /proc/cpuinfo
#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
#define kCacheLineSize (64)

    uint64_t kWriteLatencyInNS=10;
    uint64_t clflushCount=0;

    static inline void CPUPause(void) {
        __asm__ volatile("pause":::"memory");
    }

    static inline unsigned long ReadTSC(void) {
        unsigned long var;
        unsigned int hi, lo;
        asm volatile("rdtsc":"=a"(lo),"=d"(hi));
        var = ((unsigned long long int) hi << 32) | lo;
        return var;
    }

    inline void mfence(void) {
        asm volatile("mfence":::"memory");
    }

    inline void clflush(char* data, size_t len) {
        volatile char *ptr = (char*)((unsigned long)data & (~(kCacheLineSize-1)));
        mfence();
        for (; ptr < data+len; ptr+=kCacheLineSize) {
            unsigned long etcs = ReadTSC() + (unsigned long) (kWriteLatencyInNS*CPU_FREQ_MHZ/1000);
            asm volatile("clflush %0" : "+m" (*(volatile char*)ptr));
            while (ReadTSC() < etcs) CPUPause();
            clflushCount++;
        }
        mfence();
    }

    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(global_log_->raw() + (uint64_t)raw);
        return *size;
    }

    const std::string CCEH_PATH("/mnt/pmem/cceh_pool");
    //const std::string CCEH_PATH("cceh_pool");
    const uint64_t CCEH_SIZE = 40 * 1024UL * 1024UL * 1024UL;
    const std::string LOG_PATH("/mnt/pmem/log_pool");
    //const std::string LOG_PATH("log_pool");
    const uint64_t LOG_SIZE = 60 * 1024UL * 1024UL * 1024UL;
    const size_t initialSize = 1024 * 16;

    void CCEHDB::Init() {

        global_log_ = new pm::LogStore(LOG_PATH, LOG_SIZE);
        log_ = global_log_;


        bool exists = false;
        if (access(CCEH_PATH.c_str(), 0) != 0) {
            pop_ = pmemobj_create(CCEH_PATH.c_str(), "CCEH", CCEH_SIZE, 0666);
            if (!pop_) {
                perror("pmemoj_create");
                exit(1);
            }
            HashTable_ = POBJ_ROOT(pop_, CCEH);
            D_RW(HashTable_)->initCCEH(pop_, initialSize);
        } else {
            pop_ = pmemobj_open(CCEH_PATH.c_str(), "CCEH");
            if (pop_ == NULL) {
                perror("pmemobj_open");
                exit(1);
            }
            HashTable_ = POBJ_ROOT(pop_, CCEH);
            if (D_RO(HashTable_)->crashed) {
                D_RW(HashTable_)->Recovery(pop_);
            }
            exists = true;
        }

    }

    void CCEHDB::Close() {
        pmemobj_persist(pop_, (char *) &D_RO(HashTable_)->crashed, sizeof(bool));
        pmemobj_close(pop_);
        delete log_;
    }

    int CCEHDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        //utils::Timer<double> timer;
        //timer.Start();
        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        for (auto item : values) {
           value.append(item.first + item.second);
        }
        std::string whole_value = pm::GenerateRawEntry(value);
        //double time1 = timer.End();
        //std::cout << "build key value [ " << time1 * 1000000 << " ] us\n";
        //timer.Start();
        pm::PmAddr addr = log_->Alloc(whole_key.size() + whole_value.size());
        log_->Append(addr, whole_key + whole_value);
        //std::cout << "append logs [ " << timer.End() * 1000000 << " ] us\n";
        //timer.Start();
        //TODO: the raw ptr should not be used here, use the offset instead!
        //uint64_t raw_addr = (uint64_t)(log_->raw() + addr);
        D_RW(HashTable_)->Insert(pop_, addr, (char*)(addr + whole_key.size()));
        //std::cout << "update indexes [ " << timer.End() * 1000000 << " ] us\n";
        return DB::kOK;
    }

    int CCEHDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                     std::vector<KVPair> &result) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        Key_t lookup_key = (Key_t)(lookup);
        auto ret = D_RW(HashTable_)->Get(lookup_key);
        uint64_t size = DecodeSize(ret);
        char* res = new char[size];
        memcpy(res, global_log_->raw() + (uint64_t)ret, size);
        delete[] lookup;
        delete[] res;
        return DB::kOK;
    }

    int CCEHDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        Key_t lookup_key = (Key_t)(lookup);
        auto ret = D_RW(HashTable_)->Delete(pop_, lookup_key);
        delete[] lookup;
        return DB::kOK;
    }

    int CCEHDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int CCEHDB::Scan(const std::string &table, const std::string &key, int record_count,
                     const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        return 0;
    }
}
