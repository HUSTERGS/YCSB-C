//
// Created by zzyyyww on 2021/8/27.
//

#include "viper_db.h"
#include "viper/viper.hpp"
using namespace ycsbc;
namespace viper_db{
    void ViperDB::Init(){
        const size_t initial_size = 1073741824;  // 1 GiB
        viper_ = viper::Viper<std::string, std::string>::create("viper_pool", initial_size).get();
    }

    void ViperDB::Close(){}

    int ViperDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        std::string whole_key(table + key);
        std::string whole_value;
        for (auto item : values) {
            whole_value.append(item.first + item.second);
        }
        auto client = viper_->get_client();
        client.put(whole_key, whole_value);
        return DB::kOK;
    }

    int ViperDB::Read(const std::string &table, const std::string &key,
             const std::vector<std::string> *fields,
             std::vector<KVPair> &result){
        std::string whole_key(table + key);
        std::string whole_value;
        auto client = viper_->get_client();
        client.get(whole_key, &whole_value);
        return DB::kOK;
    }

    int ViperDB::Scan(const std::string &table, const std::string &key,
             int record_count, const std::vector<std::string> *fields,
             std::vector<std::vector<KVPair>> &result){
        return DB::kOK;
    }

    int ViperDB::Update(const std::string &table, const std::string &key,
               std::vector<KVPair> &values){
        return Insert(table, key, values);
    }

    int ViperDB::Delete(const std::string &table, const std::string &key){
        std::string whole_key(table + key);
        auto client = viper_->get_client();
        client.remove(whole_key);
        return DB::kOK;
    }

}
