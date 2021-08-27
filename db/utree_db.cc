//
// Created by 張藝文 on 2021/8/26.
//

#include "utree_db.h"

namespace utree_db{
    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(raw);
        return *size;
    }

    const std::string LOG_PATH("log_pool");
    const uint64_t LOG_SIZE = 1 * 1024UL * 1024UL * 1024UL;
    const size_t initialSize = 1024 * 4;

    void uTreeDB::Init() {
        if (!inited_) {
            utree_ = new btree;
            global_log_ = new pm::LogStore(LOG_PATH, LOG_SIZE);
            log_ = global_log_;
            inited_ = true;
        } else {
            return ;
        }
    }

    void uTreeDB::Close() {
        delete utree_;
        delete log_;
        global_log_ = nullptr;
    }

    int uTreeDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
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
        utree_->insert((uint64_t)(log_->raw() + addr), (char*)(log_->raw() + addr + whole_key.size()));
        auto ret = utree_->search((uint64_t)(log_->raw() + addr));
        assert(DecodeSize(ret) == (whole_value.size() - sizeof(uint64_t)));
        return DB::kOK;
    }

    int uTreeDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);
        auto ret = utree_->search(lookup_key);
        uint64_t size = DecodeSize(ret);
        char* res = new char[size];
        memcpy(res, ret, size);
        delete[] lookup;
        delete[] res;
        return DB::kOK;
    }

    int uTreeDB::Scan(const std::string &table, const std::string &key, int record_count,
                      const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        uint64_t *buf = new uint64_t[record_count];
        memset(buf, 0, record_count * sizeof(uint64_t));

        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);

        utree_->scan(lookup_key, record_count, buf);

        for (int i = 0; i < record_count && buf[i] != 0; ++i) {
            char* tmp = (char*)buf[i];
            uint64_t size = DecodeSize(tmp);
            char *res = new char[size];
            memcpy(res, res + sizeof(uint64_t), size);
            delete[] res;
        }
        delete[] buf;

        return DB::kOK;
    }

    int uTreeDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table, key, values);
    }

    int uTreeDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);
        utree_->remove(lookup_key);
        delete[] lookup;
        return DB::kOK;
    }
}
