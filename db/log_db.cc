//
// Created by zzyyyww on 2021/8/20.
//

#include "log_db.h"

namespace ycsbc {
    const std::string PM_PATH("./pmem");
    const uint64_t PM_SIZE(1024UL * 1024UL * 1024UL);
    LogDB::LogDB() {
        log_ = new LogStore(PM_PATH, PM_SIZE);
    }

    LogDB::~LogDB() {
        delete log_;
    }

    int LogDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {

    }

    int LogDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                    std::vector<KVPair> &result) {

    }

    int LogDB::Delete(const std::string &table, const std::string &key) {

    }

    int LogDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                    std::vector<std::vector<KVPair>> &result) {

    }

    int LogDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {

    }

}
