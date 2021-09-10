
#include "clht_db.h"
#include <iostream>
#include <thread>


namespace clht_db {
    thread_local int CLHT::thread_id;
    std::atomic<int> CLHT::id_;
    void clht_db::CLHT::Init() {
        if (!clht) {
            clht = clht_create(128);
        }
    }

    void clht_db::CLHT::Close() {

    }

    int CLHT::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                   std::vector<KVPair> &result) {
        if (thread_id == 0) {
            thread_id = ++CLHT::id_;
//            thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
            clht_gc_thread_init(clht, thread_id-1);
            std::cout << "thread initialize clht: " << thread_id << std::endl;
        }
        std::string whole_key(table + key);

        clht_get(clht->ht, std::hash<std::string_view>()(std::string_view(whole_key)));
        return 0;
    }

    int CLHT::Scan(const std::string &table, const std::string &key, int record_count,
                   const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        return 0;
    }

    int CLHT::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return 0;
    }

    int CLHT::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        if (thread_id == 0) {
            thread_id = ++CLHT::id_;
            clht_gc_thread_init(clht, thread_id-1);
            std::cout << "thread initialize clht: " << thread_id << std::endl;
        }

        std::string whole_key(table + key);
        std::string whole_value;
        for (auto item : values) {
            whole_value.append(item.first + item.second);
        }


        clht_put(clht, std::hash<std::string_view>()(std::string_view(whole_key)),
                std::hash<std::string_view>()(std::string_view(whole_value)));

        return 0;
    }

    int CLHT::Delete(const std::string &table, const std::string &key) {
        return 0;
    }
}

