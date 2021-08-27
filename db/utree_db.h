//
// Created by 張藝文 on 2021/8/26.
//

#ifndef YCSB_UTREE_DB_H
#define YCSB_UTREE_DB_H

#include "core/db.h"
#include "utree.h"
#include "global_log.h"

namespace utree_db {
    class uTreeDB: public ycsbc::DB {
    public:
        uTreeDB(){}

        ~uTreeDB(){}

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
        btree* utree_;
        pm::LogStore* log_;
        bool inited_{false};
    };
}

#endif //YCSB_UTREE_DB_H
