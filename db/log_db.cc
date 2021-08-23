//
// Created by zzyyyww on 2021/8/20.
//

#include <cstring>
#include "log_db.h"

namespace ycsbc {
    const std::string PM_PATH("./pmem");
    const uint64_t PM_SIZE(1024UL * 1024UL * 1024UL);
    LogDB::LogDB() {}

    LogDB::~LogDB() {}

    void LogDB::Init() {
        log_ = new LogStore(PM_PATH, PM_SIZE);
        map_.clear();
    }

    void LogDB::Close() {
        delete log_;
    }

    int LogDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key(table + key);
        std::string whole_value;
        for(auto item : values) {
            whole_value.append(item.first + item.second);
        }
        uint64_t key_size = whole_key.size();
        uint64_t value_size = whole_value.size();

        uint64_t total_size = key_size + value_size + 2 * sizeof(uint64_t);

        char* raw_data = new char[total_size];
        memcpy(raw_data, (char*)(&key_size), sizeof(uint64_t));
        memcpy(raw_data + sizeof(uint64_t), (char*)(&value_size), sizeof(uint64_t));
        memcpy(raw_data + sizeof(uint64_t) * 2, (whole_key + whole_key).c_str(), key_size + value_size);

        auto addr = log_->Alloc(total_size);
        log_->Append(addr, std::string(raw_data, total_size));
        map_[key] = addr;

        delete[] raw_data;
        return DB::kOK;
    }

    int LogDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {
        PmAddr value_pos = map_[key];
        char* raw_kv = log_->raw() + value_pos;
        uint64_t *key_size, *value_size;
        key_size = (uint64_t*)raw_kv;
        value_size = (uint64_t*)(raw_kv + sizeof(uint64_t));

        char* raw_value = raw_kv + (*key_size + 2 * sizeof(uint64_t));
        std::string value(raw_value, *value_size);
        return DB::kOK;
    }

    int LogDB::Delete(const std::string &table, const std::string &key) {
        map_.erase(table + key);
        return DB::kOK;
    }

    int LogDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {
        return DB::kOK;
    }

    int LogDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        Insert(table, key, values);
        return DB::kOK;
    }

}
