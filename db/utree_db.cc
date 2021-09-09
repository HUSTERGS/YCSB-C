//
// Created by zzyyyww on 2021/8/26.
//

#include "utree_db.h"

namespace utree_db{
    inline uint64_t DecodeSize(const char* raw) {
        if (raw == nullptr) {
            return 0;
        }
        uint64_t* size = (uint64_t*)(raw);
        return *size;
    }

    const std::string LOG_PATH("/mnt/pmem/log_pool");
    const uint64_t LOG_SIZE = 60 * 1024UL * 1024UL * 1024UL;
    const std::string UTREE_PATH("/mnt/pmem/utree_pool");
    const uint64_t UTREE_SIZE = 40 * 1024UL * 1024UL * 1024UL;

    size_t mapped_len;
    int is_pmem;

    void uTreeDB::Init() {
        if (!inited_) {

#ifndef USE_PMDK
            start_addr = (char *) pmem_map_file(UTREE_PATH.c_str(), UTREE_SIZE, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
            if (start_addr == nullptr) {
                fprintf(stderr, "map file failed [%s]\n", strerror(errno));
                exit(-1);
            }
            curr_addr = start_addr;
            printf("start_addr=%p, end_addr=%p\n", start_addr,
                   start_addr + (uint64_t)40ULL * 1024ULL * 1024ULL * 1024ULL);
#endif

            utree_ = new btree;
            global_log_ = new pm::LogStore(LOG_PATH, LOG_SIZE);
            log_ = global_log_;
            inited_ = true;
        } else {
            return ;
        }
    }

    void uTreeDB::Close() {
        pmem_unmap(start_addr, mapped_len);
        delete utree_;
        delete log_;
        global_log_ = nullptr;
    }

    int uTreeDB::Insert(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        for (auto item : values) {
            value.append(item.first + item.second);
        }
        std::string whole_value = pm::GenerateRawEntry(value);
        pm::PmAddr addr = log_->Alloc(whole_key.size() + whole_value.size());
        log_->Append(addr, whole_key + whole_value);
        //TODO: the raw ptr should not be used here, use the offset instead!
        //uint64_t raw_addr = (uint64_t)(log_->raw() + addr);
        //printf("insert [%s]\n", key.c_str());
        utree_->insert((uint64_t)(log_->raw() + addr), (char*)(log_->raw() + addr + whole_key.size()));
        auto ret = utree_->search((uint64_t)(log_->raw() + addr));
        assert(DecodeSize(ret) == (whole_value.size() - sizeof(uint64_t)));
        return DB::kOK;
    }

    int uTreeDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        std::string value;
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);
        auto ret = utree_->search(lookup_key);
        uint64_t size = DecodeSize(ret);
        char* res = new char[size];
        memcpy(res, ret, size);
        delete[] lookup;
        delete[] res;
        return DB::kOK;
    }

    int uTreeDB::Scan(const std::string &table, const std::string &key, int record_count,
                      const std::vector<std::string> *fields, std::vector<std::vector<KVPair>> &result) {
        uint64_t *buf = new uint64_t[record_count];
        memset(buf, 0, record_count * sizeof(uint64_t));

        std::string whole_key = pm::GenerateRawEntry(table + key);
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);

        list_node_t* node = utree_->scan(lookup_key);

        const int LEN_SIZE = sizeof(uint64_t);

        list_node_t* n = node;
        std::string tkey, tvalue;
        uint64_t key_size = DecodeSize((char*)node->key);
        tkey = std::string((char*)node->key + LEN_SIZE, key_size);
        uint64_t value_size = DecodeSize((char*)node->ptr);
        tvalue = std::string((char*)node->ptr + LEN_SIZE, value_size);
        //printf("scan start at [%s]\n", tkey.c_str());

        if (node!= nullptr) {
            for (int i = 0; i < record_count && n != nullptr; n = n->next, i++) {
                key_size = DecodeSize((char*)n->key);
                value_size = DecodeSize((char*)n->ptr);
                tkey = std::move(std::string((char*)n->key + LEN_SIZE, key_size));
                tvalue = std::move(std::string((char*)n->ptr + LEN_SIZE, value_size));
                //printf("scan get key [%s]\n", tkey.c_str());
            }
        }
        return DB::kOK;
    }

    int uTreeDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Delete(table, key);
    }

    int uTreeDB::Delete(const std::string &table, const std::string &key) {
        std::string whole_key = pm::GenerateRawEntry(table + key);
        char* lookup = new char[whole_key.size()];
        memcpy(lookup, whole_key.c_str(), whole_key.size());
        entry_key_t lookup_key = (entry_key_t)(lookup);
        utree_->remove(lookup_key);
        delete[] lookup;
        return DB::kOK;
    }
}
