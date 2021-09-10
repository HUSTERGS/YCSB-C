#pragma once

#include "core/db.h"
#include <atomic>
#ifdef __cplusplus
extern "C"
{
#endif

// C header here
#include <immintrin.h>
#include "clht.h"

#ifdef __cplusplus
}
#endif



namespace clht_db {
    class CLHT: public ycsbc::DB {
    public:
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
        static std::atomic<int> id_;
    private:
        clht_t * clht;
        static thread_local int thread_id;

    };
}