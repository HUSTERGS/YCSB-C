//
// Created by 張藝文 on 2021/9/8.
//

#include "metakv_db.h"

namespace ycsb_metakv{
    void ycsbMetaKV::Init() {
        Options options;
        options.cceh_file_size = 32UL * 1024 * 1024 * 1024;
        options.data_file_size = 128UL * 1024 * 1024 * 1024;
        db = MetaDB{};
        db.Open(options, "/mnt/pmem/metakv");
    }

    void ycsbMetaKV::Close() {}

    int ycsbMetaKV::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key(table + key);
        std::string whole_value;
        for (auto item : values) {
            whole_value.append(item.first + item.second);
        }
        ycsbKey internal_key(whole_key.substr(0, whole_key.find('-')),
                    whole_key.substr(whole_key.find('-'), whole_key.size()));
        ycsbValue internal_value(whole_value);
        //printf("insert %s\n", (table + key).c_str());
        bool res = db.Put(internal_key, internal_value);
        if (res) return DB::kOK;
        return DB::kErrorNoData;
    }

    int ycsbMetaKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                         std::vector<KVPair> &result) {
        std::string whole_key(table + key);
        ycsbValue value;
        ycsbKey internal_key(whole_key.substr(0, whole_key.find('-')),
                             whole_key.substr(whole_key.find('-'), whole_key.size()));
        bool res = db.Get(internal_key, value);
        if (res) {
            return DB::kOK;
        }
        return DB::kErrorNoData;
    }

    int ycsbMetaKV::Delete(const std::string &table, const std::string &key) {
        return DB::kOK;
    }

    int ycsbMetaKV::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return DB::kOK;
    }

    int ycsbMetaKV::Scan(const std::string &table, const std::string &key, int record_count,
                         const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        return DB::kOK;
    }
}
