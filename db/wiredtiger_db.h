#pragma once

#include "core/db.h"
#include "wiredtiger.h"

namespace ycsbc {
  class WiredTigerDB : public DB {
  public:
      void Init();
      void Close();
      int Read(const std::string &table, const std::string &key,
               const std::vector<std::string> *fields,
               std::vector<KVPair> &result);
      int Scan(const std::string &table, const std::string &key,
               int len, const std::vector<std::string> *fields,
               std::vector<std::vector<KVPair>> &result);
      int Update(const std::string &table, const std::string &key,
                 std::vector<KVPair> &values);
      int Insert(const std::string &table, const std::string &key,
                 std::vector<KVPair> &values);
      int Delete(const std::string &table, const std::string &key);
  private:
      static std::string get_whole_value(std::vector<KVPair> &values);

      void thread_init();
      WT_CONNECTION *conn = nullptr;
      thread_local static WT_SESSION *session;
//      thread_local static WT_CURSOR *cursor;
  };
};