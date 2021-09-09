//
// Created by zzyyyww on 2021/9/4.
//

#include "hikvdb.h"

namespace hikvdb {
    void HiKVDB::Init() {
        open_hikv::OpenHiKV::OpenPlainVanillaOpenHiKV(&hikv_);
    }

    void HiKVDB::Close() {}

    int HiKVDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = table + key;
        std::string value;
        for (auto item : values) {
            value.append(item.first + item.second);
        }
        hikv_->Set(whole_key, value);
        return DB::kOK;
    }

    int HiKVDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                     std::vector<KVPair> &result) {
        std::string whole_key = table + key;
        open_hikv::Slice value;
        open_hikv::ErrorCode e = hikv_->Get(whole_key, &value);
        if (e == open_hikv::ErrorCode::kNotFound) {
            return kErrorNoData;
        } else {
            return kOK;
        }
    }

    int HiKVDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = table + key;
        hikv_->Del(whole_key);
        return kOK;
    }

    int HiKVDB::Scan(const std::string &table, const std::string &key, int record_count,
                     const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        return kOK;
    }

    int HiKVDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }
}
