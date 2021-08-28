//
// Created by zzyyyww on 2021/8/27.
//

#ifndef YCSB_VIPER_DB_H
#define YCSB_VIPER_DB_H

#include <string>
#include <memory>
//#include "viper/viper.hpp"
#include "core/db.h"

namespace viper_db {
    class ViperDB: public ycsbc::DB {
    public:
        ViperDB(){}

        ~ViperDB(){}

        void Init();

        void Close();

        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int record_count, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Delete(const std::string &table, const std::string &key);

    private:
    };
}
#endif //YCSB_VIPER_DB_H
