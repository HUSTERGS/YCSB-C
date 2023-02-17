#include "wiredtiger_db.h"

#define DATA_HOME "/Users/samuel/git-repos/YCSB-C/data"

namespace ycsbc {
    // 共用一个connection，但是每个线程使用单独的session以及cursor
    thread_local WT_SESSION * WiredTigerDB::session = nullptr;
//    thread_local WT_CURSOR * WiredTigerDB::cursor = nullptr;

    void WiredTigerDB::thread_init() {
        if (!session) {
            conn->open_session(conn, nullptr, nullptr, &session);
        }
//        if (!cursor) {
//            session->open_cursor(session, "table:test", nullptr, nullptr, &cursor);
//        }
    }

    std::string WiredTigerDB::get_whole_value(std::vector<KVPair> &values) {
        std::string whole_value;
        for (const auto &item : values) {
            // 第一个字段是field，不要field
            whole_value.append(item.second);
        }
        return whole_value;
    }

    void WiredTigerDB::Init() {
        const char * home = DATA_HOME;
        wiredtiger_open(home, nullptr, "create", &conn);

        WT_SESSION * s;
        conn->open_session(conn, nullptr, nullptr, &s);
        // 创建表
        s->create(s, "table:test", "key_format=S,value_format=S");
        s->close(s, nullptr);
    }

    void WiredTigerDB::Close() {
        WT_SESSION * s;
        conn->open_session(conn, nullptr, nullptr, &s);
        s->drop(s, "table:test", nullptr);
        s->close(s, nullptr);
        conn->close(conn, nullptr);
    }

    int WiredTigerDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        thread_init();

        WT_CURSOR *cursor;
        session->open_cursor(session, "table:test", nullptr, nullptr, &cursor);
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, get_whole_value(values).c_str());
        int ret = cursor->insert(cursor);
        cursor->close(cursor);
        return ret;
    }

    int WiredTigerDB::Delete(const std::string &table, const std::string &key) {
        thread_init();

        WT_CURSOR *cursor;
        session->open_cursor(session, "table:test", nullptr, nullptr, &cursor);
        cursor->set_key(cursor, key.c_str());
        int ret = cursor->remove(cursor);
        cursor->close(cursor);
        return ret;
    }

    int WiredTigerDB::Read(const std::string &table, const std::string &key, const std::vector <std::string> *fields,
                           std::vector <KVPair> &result) {
        thread_init();

        WT_CURSOR *cursor;
        session->open_cursor(session, "table:test", nullptr, nullptr, &cursor);
        cursor->set_key(cursor, key.c_str());
        cursor->search(cursor);
        const char *value = nullptr;

        cursor->get_value(cursor, &value);
        if (!value) {
            return DB::kErrorNoData;
        }
        result.emplace_back("", value);

        cursor->close(cursor);
        return DB::kOK;
    }


    int WiredTigerDB::Scan(const std::string &table, const std::string &key, int len,
                           const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        thread_init();
        // TODO
        return 0;
    }

    int WiredTigerDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        thread_init();

        WT_CURSOR *cursor;
        session->open_cursor(session, "table:test", nullptr, nullptr, &cursor);
        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, get_whole_value(values).c_str());
        int ret = cursor->update(cursor);
        cursor->close(cursor);
        return ret;
    }

}

