//
// Created by zzyyyww on 2021/8/23.
//

#ifndef YCSB_CCEH_DB_H
#define YCSB_CCEH_DB_H

#include <libpmemobj.h>

#include "core/db.h"
//#include "lib/pm_log_store.h"
#include "CCEH.h"

namespace cceh_db {
    class CCEHDB : public ycsbc::DB {
    public:
       CCEHDB(){}

       ~CCEHDB(){}

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
        TOID(CCEH) HashTable_ {OID_NULL};
        PMEMobjpool *pop_;
        pm::LogStore* log_ {nullptr};
    };
}

#endif //YCSB_CCEH_DB_H
