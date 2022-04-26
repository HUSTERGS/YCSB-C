//
// Created by 張藝文 on 2021/9/8.
//

#include "metakv_db.h"

namespace ycsb_metakv{
    void ycsbMetaKV::Init() {
        struct DBOption db_option{};
        SetDefaultDBop(&db_option);
//#ifdef DMETAKV_CACHE
//        SetCacheOp(1, 1, (100ul * (1ul << 20)), (100ul * (1ul << 20)))
//#endif
        db_option.stat_options.compression = true;
        db_option.stat_options.max_wbl_num = 1000;
        db_option.stat_options.counter_policy_max_entry = 1000;


        DBOpen(&db_option,"/mnt/pmem01/metakv", &db);
    }

    void ycsbMetaKV::Close() {
        DBExit(&db);
    }

    int ycsbMetaKV::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        uint64_t pinode = *(uint64_t *)(key.data());
        Slice fname;
        SliceInit(&fname, 17, const_cast<char *>(key.data() + sizeof(uint64_t)));

        uint64_t inode = *(uint64_t *)(values.at(0).first.data());
        std::string stat_str = values.at(0).second;

        if (MetaKVPut(&db, pinode, &fname, inode, (struct stat *)stat_str.data()) != OK) {
            return DB::kErrorNoData;
        }

        return DB::kOK;
    }

    int ycsbMetaKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                         std::vector<KVPair> &result) {
        uint64_t pinode = *(uint64_t *)(key.data());
        Slice fname;
        SliceInit(&fname, 17, const_cast<char *>(key.data() + sizeof(uint64_t)));
        uint64_t inode = 0;
        Slice stat_slice;


        if (MetaKVGet(&db, pinode, &fname, &inode, &stat_slice) != OK) {
            return DB::kErrorNoData;
        }
        free(stat_slice.data);
        return DB::kOK;
    }

    int ycsbMetaKV::Delete(const std::string &table, const std::string &key) {
        uint64_t pinode = *(uint64_t *)(key.data());
        Slice fname;
        SliceInit(&fname, 17, const_cast<char *>(key.data() + sizeof(uint64_t)));

        if (MetaKVDelete(&db, pinode, &fname) != 0) {
            return DB::kErrorNoData;
        }
        return DB::kOK;
    }

    int ycsbMetaKV::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        // for workload a/b/c/d/e/f should use insert instead of delete
//        return Insert(table, key, values);
        return Delete(table, key);
    }

    int ycsbMetaKV::Scan(const std::string &table, const std::string &key, int record_count,
                         const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        uint64_t pinode = *(uint64_t *)(key.data());
        Slice fname;
        SliceInit(&fname, 17, const_cast<char *>(key.data() + sizeof(uint64_t)));

        char * r = nullptr;
        if (MetaKVScan(&db, pinode, &r) != 0 || r == nullptr) {
            return DB::kErrorNoData;
        }
        free(r);
        return DB::kOK;
    }
}
