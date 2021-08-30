//
// Created by zzyyyww on 2021/8/23.
//

#include <unistd.h>
#include "cceh_db.h"
#include "global_log.h"

namespace cceh_db {

    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(global_log_->raw() + (uint64_t)raw);
        return *size;
    }

    //const std::string CCEH_PATH("/pmem0/zyw/cceh_pool");
    const std::string CCEH_PATH("cceh_pool");
    const uint64_t CCEH_SIZE = 1 * 1024UL * 1024UL * 1024UL;
    //const std::string LOG_PATH("/pmem0/zyw/log_pool");
    const std::string LOG_PATH("log_pool");
    const uint64_t LOG_SIZE = 1 * 1024UL * 1024UL * 1024UL;
    const size_t initialSize = 1024 * 4;

    void CCEHDB::Init() {
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
        global_log_ = new pm::LogStore(LOG_PATH, LOG_SIZE);
        log_ = global_log_;
    }

    void CCEHDB::Close() {
        pmemobj_persist(pop_, (char *) &D_RO(HashTable_)->crashed, sizeof(bool));
        pmemobj_close(pop_);
        delete log_;
    }

    int CCEHDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        for (auto item : values) {
           value.append(item.first + item.second);
        }
        std::string whole_value = pm::GenerateRawEntry(value);
        pm::PmAddr addr = log_->Alloc(whole_key.size() + whole_value.size());
        log_->Append(addr, whole_key + whole_value);
        //TODO: the raw ptr should not be used here, use the offset instead!
        //uint64_t raw_addr = (uint64_t)(log_->raw() + addr);
        D_RW(HashTable_)->Insert(pop_, addr, (char*)(addr + whole_key.size()));
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
