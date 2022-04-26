//
// Created by zzyyyww on 2021/8/23.
//

#include "pmem_rocksdb_db.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "rocksdb/env.h"
#include "rocksdb/slice.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/iterator.h"

using namespace ycsbc;

namespace ycsb_pmem_rocksdb{

    const std::string PMEM_PATH("/mnt/pmem01/");
    const std::string DB_NAME("/mnt/pmem01/rocksdb/");
    const uint64_t PMEM_SIZE = 60 * 1024UL * 1024UL * 1024UL;

    void PmemRocksDB::Init() {
        rocksdb::Options options;

        options.max_background_jobs = 2;

        options.create_if_missing = true;
        options.dcpmm_kvs_enable = true;
        options.dcpmm_kvs_mmapped_file_fullpath = PMEM_PATH;
        options.dcpmm_kvs_mmapped_file_size = PMEM_SIZE;
        options.recycle_dcpmm_sst = true;

        options.env = rocksdb::NewDCPMMEnv(rocksdb::DCPMMEnvOptions());
//        options.use_fsync = true;
        options.write_buffer_size = 32 << 20; // 32M



        // options from HWDB
//        options.max_bytes_for_level_base = 32 << 20;
//        options.max_write_buffer_number = 2;
//        options.target_file_size_base = 4 << 20;
//
//        options.level0_slowdown_writes_trigger = 8;
//        options.level0_stop_writes_trigger = 12;

//        options.use_direct_reads = true;
//        options.use_direct_io_for_flush_and_compaction = true;

        // 统计数据
//        options.statistics = rocksdb::CreateDBStatistics();

        // 压缩相关
//        options.compression = rocksdb::kNoCompression;
        options.dcpmm_compress_value = true;

        rocksdb::Status s = rocksdb::DB::Open(options, DB_NAME, &db_);
        if (!s.ok()) {
            fprintf(stderr, "Open rocksdb failed [%s]\n", s.ToString().c_str());
            exit(-1);
        }
    }

    void PmemRocksDB::Close() {
        db_->Close();
        delete db_;
        db_ = nullptr;
    }

    int PmemRocksDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        const std::string& whole_key = key;
        std::string whole_value;

        static volatile uint64_t pinode = *(uint64_t *)(key.data());
        static volatile rocksdb::Slice fname(const_cast<char *>(key.data() + sizeof(uint64_t)), 17);
        static volatile uint64_t inode = *(uint64_t *)(values.at(0).first.data());
        static volatile std::string stat_str = values.at(0).second;

        for (const auto& item : values) {
            whole_value.append(item.first + item.second);
        }
//        printf("rocksdb whole value size = %lu", whole_value.size());
        rocksdb::WriteOptions options;
        options.sync = true;
        rocksdb::Status s = db_->Put(options, rocksdb::Slice(whole_key), rocksdb::Slice(whole_value));
        if (s.ok()) {
            return DB::kOK;
        } else {
            fprintf(stderr, "put failed[%s]\n", s.ToString().c_str());
            return DB::kOK;
        }
    }

    int PmemRocksDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                          std::vector<KVPair> &result) {
        const std::string& whole_key = key;
        std::string raw_value;

        static volatile uint64_t pinode = *(uint64_t *)(key.data());
        static volatile rocksdb::Slice fname(const_cast<char *>(key.data() + sizeof(uint64_t)), 17);


        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), rocksdb::Slice(whole_key), &raw_value);
        if (s.IsNotFound()) {
            return DB::kErrorNoData;
        }
        return DB::kOK;
    }

    int PmemRocksDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int PmemRocksDB::Delete(const std::string &table, const std::string &key) {
        const std::string& whole_key = key;

        static volatile uint64_t pinode = *(uint64_t *)(key.data());
        static volatile rocksdb::Slice fname(const_cast<char *>(key.data() + sizeof(uint64_t)), 17);

        rocksdb::WriteOptions options;
        options.sync = true;
        rocksdb::Status s = db_->Delete(options, rocksdb::Slice(whole_key));
        return DB::kOK;
    }

    int PmemRocksDB::Scan(const std::string &table, const std::string &key, int record_count,
                          const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        std::string whole_key = table + key;
        uint64_t pinode = *(uint64_t *)(key.data());
        static volatile rocksdb::Slice fname(const_cast<char *>(key.data() + sizeof(uint64_t)), 17);


        rocksdb::Iterator *iter = db_->NewIterator(rocksdb::ReadOptions());
//        std::vector<std::string> raw_values;

        iter->Seek(rocksdb::Slice(table + std::string{reinterpret_cast<const char*>(&pinode), sizeof(pinode)}));
        for(int i = 0; i < 200 && iter->Valid(); i++, iter->Next()) {
//            raw_values.push_back(iter->value().ToString());
//            std::string tmp_key = iter->key().ToString();
//            uint64_t tmp_pinode = *(uint64_t *)(tmp_key.data() + table.size());
            static volatile std::string raw_values = iter->value().ToString();
//            uint64_t inode = *(uint64_t *)(raw_values.data());
//            printf("i = %d, pinode = %lu, scan get pinode = %lu\n",i, pinode, tmp_pinode);
        }
        delete iter;
//        exit(0);
        return DB::kOK;
    }
}
