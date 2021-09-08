#pragma once

#include "core/db.h"
#include "MetaDB.h"

namespace metakv {
    class MetaKV: public ycsbc::DB {
        public:
            MetaKV(){};
            ~MetaKV(){};
            void Init();
            void Close();
            int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result) override;
            int Scan(const std::string &table, const std::string &key,
                 int record_count, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result) override;

            int Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) override;

            int Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) override;

            int Delete(const std::string &table, const std::string &key);
        private:
            MetaDB * db;
    };
}