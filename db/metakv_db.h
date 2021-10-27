//
// Created by 張藝文 on 2021/9/8.
//

#ifndef YCSB_METALV_DB_H
#define YCSB_METALV_DB_H

#include "core/db.h"
#ifdef __cplusplus
extern "C" {
#include "include/metadb.h"
};

#endif

namespace ycsb_metakv {

    class ycsbMetaKV : public ycsbc::DB {
    public:
        ycsbMetaKV() {}

        ~ycsbMetaKV() {}

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
        MetaDb db;
    };
}
#endif //YCSB_METALV_DB_H
