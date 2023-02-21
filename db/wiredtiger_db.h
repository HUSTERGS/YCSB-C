#pragma once

#include "core/db.h"
#include "wiredtiger.h"
#include <sstream>

namespace ycsbc {
  class WiredTigerDB : public DB {
  public:
      WiredTigerDB() {
          const char * home = DATA_HOME;
          init_config();
          wiredtiger_open(home, nullptr, wt_open_config.str().c_str(), &conn);

          WT_SESSION * s;
          conn->open_session(conn, nullptr, nullptr, &s);
          // 创建表
          s->create(s, "table:test", session_create_config.str().c_str());
          s->close(s, nullptr);
      }

      ~WiredTigerDB() {
          WT_SESSION * s;
          conn->open_session(conn, nullptr, nullptr, &s);
          s->drop(s, "table:test", nullptr);
          s->close(s, nullptr);
          conn->close(conn, nullptr);
      }
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

      // Number of bytes to use as a cache of uncompressed data.
      // Negative means use default settings.
      // rocksdb的write_buffer_size为64m

//      constexpr static auto FLAGS_cache_size = (64 * 1024 * 1024);
      constexpr static auto FLAGS_cache_size = (3ull * 1024 * 1024 * 1024);

      // Use log and checkpoint
      // 0: close
      // 1: open (async)
      // 2: open (sync)
      constexpr static auto FLAGS_use_log = 0;

      constexpr static auto FLAGS_internal_page_max = (char *)"16kb";
      constexpr static auto FLAGS_leaf_page_max = (char *)"16kb";

      void init_config();
      static std::string get_whole_value(std::vector<KVPair> &values);
      std::stringstream wt_open_config;
      std::stringstream session_create_config;
      const char *DATA_HOME = "/Users/samuel/git-repos/YCSB-C/data";
      WT_CONNECTION *conn = nullptr;
      thread_local static WT_SESSION *session;
      thread_local static WT_CURSOR *cursor;
  };
};