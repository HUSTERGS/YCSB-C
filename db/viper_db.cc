//
// Created by zzyyyww on 2021/8/27.
//
#include <unordered_map>
#include <sys/syscall.h>
#include <mutex>
#include "viper_db.h"
#include "viper/viper.hpp"
#include "core/timer.h"
using namespace ycsbc;
namespace viper_db{

    std::unique_ptr<viper::Viper<std::string, std::string>> global_viper_;
    std::unordered_map<int, viper::Viper<std::string, std::string>::Client> client_map_;
    std::mutex mu;

    void ViperDB::Init(){
        if (global_viper_ == nullptr) {
            const size_t initial_size = 1073741824;  // 1 GiB
            global_viper_ = viper::Viper<std::string, std::string>::create("/mnt/pmem/viper_pool", initial_size);
        }
    }

    void ViperDB::Close(){
        if (global_viper_ == nullptr) {
            global_viper_.release();
        }
    }

    int ViperDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        //utils::Timer<double> timer;
        //timer.Start();
        int tid = syscall(SYS_gettid);
        mu.lock();
        auto client = client_map_.find(tid);
        if (client == client_map_.end()) {
            client_map_.insert({tid, global_viper_->get_client()});
            client = client_map_.find(tid);
        }
        mu.unlock();
        //std::cout << "get client [ " << timer.End() * 1000000 << " ] us\n";
        //timer.Start();
        std::string whole_key(table + key);
        std::string whole_value;
        for (auto item : values) {
            whole_value.append(item.first + item.second);
        }
        //std::cout << "build key value [ " << timer.End() * 1000000 << " ] us\n";
        //timer.Start();
        client->second.put(whole_key, whole_value);
        //std::cout << "insert [ " << timer.End() * 1000000 << " ] us\n";
        return DB::kOK;
    }

    int ViperDB::Read(const std::string &table, const std::string &key,
             const std::vector<std::string> *fields,
             std::vector<KVPair> &result){
        int tid = syscall(SYS_gettid);
        mu.lock();
        auto client = client_map_.find(tid);
        if (client == client_map_.end()) {
            client_map_.insert({tid, global_viper_->get_client()});
            client = client_map_.find(tid);
        }
        mu.unlock();
        std::string whole_key(table + key);
        std::string whole_value;
        client->second.get(whole_key, &whole_value);
        return DB::kOK;
    }

    int ViperDB::Scan(const std::string &table, const std::string &key,
             int record_count, const std::vector<std::string> *fields,
             std::vector<std::vector<KVPair>> &result){
        return DB::kOK;
    }

    int ViperDB::Update(const std::string &table, const std::string &key,
               std::vector<KVPair> &values){
        return Delete(table, key);
    }

    int ViperDB::Delete(const std::string &table, const std::string &key){
        int tid = syscall(SYS_gettid);
        auto client = client_map_.find(tid);
        if (client == client_map_.end()) {
            client_map_.insert({tid, global_viper_->get_client()});
            client = client_map_.find(tid);
        }
        std::string whole_key(table + key);
        client->second.remove(whole_key);
        return DB::kOK;
    }

}
