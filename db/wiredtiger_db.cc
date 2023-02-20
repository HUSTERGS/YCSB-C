#include "wiredtiger_db.h"
#include <cassert>

namespace ycsbc {
    // 共用一个connection，但是每个线程使用单独的session以及cursor
    thread_local WT_SESSION * WiredTigerDB::session = nullptr;
    thread_local WT_CURSOR * WiredTigerDB::cursor = nullptr;

    std::string WiredTigerDB::get_whole_value(std::vector<KVPair> &values) {
        std::string whole_value;
        for (const auto &item : values) {
            // 第一个字段是field，不要field
            whole_value.append(item.second);
        }
        return whole_value;
    }
    // 郭老师的代码
    void WiredTigerDB::init_config() {
#define SMALL_CACHE (10 * 1024 * 1024)

        std::stringstream config;
        wt_open_config.str("");
        wt_open_config << "create";
        wt_open_config << ",cache_size=" << FLAGS_cache_size;
        // 后台线程固定为8
        wt_open_config << ",eviction=(threads_min=8,threads_max=8)";
        /* TODO: Translate write_buffer_size - maybe it's chunk size?
        options.write_buffer_size = FLAGS_write_buffer_size;
        */

        wt_open_config << ",log=(enabled=" << (FLAGS_use_log > 0 ? "true" : "false") << ")";
        if (FLAGS_use_log == 2)
            wt_open_config << ",transaction_sync=(enabled=true)";

        // Create tuning options and create the data file
        session_create_config.str("");
        session_create_config << "key_format=S,value_format=S";
        session_create_config << ",prefix_compression=false";
        session_create_config << ",checksum=off";
        if (FLAGS_cache_size < SMALL_CACHE)
        {
            session_create_config << ",internal_page_max=" << FLAGS_internal_page_max;
            session_create_config << ",leaf_page_max=" << FLAGS_leaf_page_max;
            session_create_config << ",memory_page_max=" << SMALL_CACHE;
        }
        else
        {
            long long int memmax = FLAGS_cache_size * 0.9;
            session_create_config << ",internal_page_max=" << (char *)"16kb";
            session_create_config << ",leaf_page_max=" << (char *)"16kb";
            session_create_config << ",memory_page_max=" << memmax;
        }
    }

    void WiredTigerDB::Init() {
        if (!session) {
            conn->open_session(conn, nullptr, nullptr, &session);
        }
        if (!cursor) {
            session->open_cursor(session, "table:test", nullptr, nullptr, &cursor);
        }
    }

    void WiredTigerDB::Close() {
        if (cursor) {
            cursor->close(cursor);
            cursor = nullptr;
        }
        if (session) {
            session->close(session, nullptr);
            session = nullptr;
        }
    }

    int WiredTigerDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        assert(get_whole_value(values).length() == 8);
        assert(key.length() == 8);
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, get_whole_value(values).c_str());
        int ret = cursor->insert(cursor);
        return ret;
    }

    int WiredTigerDB::Delete(const std::string &table, const std::string &key) {
        cursor->set_key(cursor, key.c_str());
        int ret = cursor->remove(cursor);
        return ret;
    }

    int WiredTigerDB::Read(const std::string &table, const std::string &key, const std::vector <std::string> *fields,
                           std::vector <KVPair> &result) {
        cursor->set_key(cursor, key.c_str());
        cursor->search(cursor);
        const char *value = nullptr;

        cursor->get_value(cursor, &value);
        if (!value) {
            return DB::kErrorNoData;
        }
        result.emplace_back("", value);
        return DB::kOK;
    }


    int WiredTigerDB::Scan(const std::string &table, const std::string &key, int len,
                           const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        // TODO
        return 0;
    }

    int WiredTigerDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, get_whole_value(values).c_str());
        int ret = cursor->update(cursor);
        return ret;
    }

}

