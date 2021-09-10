#include "metakv_db.h"

namespace metakv {

    void MetaKV::Init() {
        Options db_options;
        db_options.cceh_file_size = (1 << 30) * 32UL; // 100M
        db_options.data_file_size = (1 << 30) * 64UL; // 100M
        if (!db) {
            db = new MetaDB();
            db->Open(db_options, "/mnt/pmem/metakv");
        }
    }

    void MetaKV::Close() {
        
    }

    int MetaKV::Insert( const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key(table + key);
        std::string whole_value;
        for (auto item : values) {
            whole_value.append(item.first + item.second);
        }
        ycsbKey internal_key(whole_key.substr(0, whole_key.find('-')),
                             whole_key.substr(whole_key.find('-'), whole_key.size()));
        ycsbValue internal_value(whole_value);
        bool res = db->Put(internal_key, internal_value);
        if (res) return DB::kOK;
        return DB::kErrorNoData;
    }

    int MetaKV::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields, std::vector<KVPair> &result) {
        std::string whole_key(table + key);
        ycsbValue value;
        ycsbKey internal_key(whole_key.substr(0, whole_key.find('-')),
                             whole_key.substr(whole_key.find('-'), whole_key.size()));
        bool res = db->Get(internal_key, value);
        if (res) {
            return DB::kOK;
        }
        return DB::kErrorNoData;
    }

    int MetaKV::Scan(const std::string &table, const std::string &key, int record_count, const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        auto find = key.find_last_of('-');
        if (find == std::string::npos) {
            std::cout << "解析key出错" << std::endl;
            return -1;
            assert(0);
        }
        // "user-"
        Slice prefix{key.data() + 5, find - 5};
        Slice suffix{key.data() + 6 + prefix.size(), key.size() - find - 1};
        // 0123456789
        // user-ab-cd
        // const SliceKey k = SliceKey(prefix, suffix);
        std::vector<LogEntryRecord> kvp;
        
        db->Scan(prefix, kvp);

        return DB::kOK;
    }

    int MetaKV::Delete(const std::string &table, const std::string &key) {
        auto find = key.find_last_of('-');
        if (find == std::string::npos) {
            std::cout << "解析key出错" << std::endl;
            return -1;
            assert(0);
        }
        // "user-"
        Slice prefix{key.data() + 5, find - 5};
        Slice suffix{key.data() + 6 + prefix.size(), key.size() - find - 1};
        // 0123456789
        // user-ab-cd
        SliceKey k(&prefix, &suffix);
        
        db->Delete(k);

        return DB::kOK;
    }
    
    int MetaKV::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        auto find = key.find_last_of('-');
        if (find == std::string::npos) {
            std::cout << "解析key出错" << std::endl;
            return -1;
            assert(0);
        }
        // "user-"
        Slice prefix{key.data() + 5, find - 5};
        Slice suffix{key.data() + 6 + prefix.size(), key.size() - find - 1};
        // 0123456789
        // user-ab-cd
        SliceKey k(&prefix, &suffix);
        std::string value;
        for (auto item : values) {
           value.append(item.first + item.second);
        }
        SliceValue v(Slice(value.data(), value.size()));
        
        db->Update(k, v);

        return DB::kOK;
    }
}