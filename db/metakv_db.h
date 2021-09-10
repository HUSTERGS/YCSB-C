#pragma once

#include "core/db.h"
#include "MetaDB.h"

namespace metakv {
    struct ycsbKey: public MetaKey {

        std::string prefix_;
        std::string suffix_;

        ycsbKey(std::string prefix, std::string suffix):
                prefix_(std::move(prefix)),
                suffix_(std::move(suffix)){}

        size_t EncodeSize() override {
            return prefix_.size() + suffix_.size() + 2 * sizeof(uint64_t);
        }
        void Encode() override {
            buff_.clear();
            PutFixed64(&buff_, prefix_.size());
            buff_.append(prefix_);
            PutFixed64(&buff_, suffix_.size());
            buff_.append(suffix_);
            internal_key_ = Slice(buff_);
        }
        void Decode() override {
            Slice temp(internal_key_);
            uint64_t prefix_size = GetFixed64(&temp);
            prefix_ = std::string(temp.data(), prefix_size);
            temp = Slice(temp.data() + prefix_size);
            uint64_t suffix_size = GetFixed64(&temp);
            suffix_ = std::string(temp.data(), suffix_size);
        }
        void GetPrefix(Slice * buff) override {
            *buff = Slice(prefix_);
        }
        void GetSuffix(Slice * buff) override {
            *buff = Slice(suffix_);
        }
    };

    struct ycsbValue : public MetaValue {
        std::string value;
        ycsbValue(std::string v) : value(std::move(v)) {}
        ycsbValue() =default;
        void GetValue(Slice * buff) override {
            *buff = Slice(internal_value_);
        }
        void Decode() override {
            value = internal_value_.ToString();
        }
        void Encode() override {
            buff_ = value;
            internal_value_ = Slice(buff_);
        }
    };

    class MetaKV: public ycsbc::DB {
        public:

            void Init();
            void Close();
            int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result) override;
            int Scan(const std::string &table, const std::string &key,
                 int record_count, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result) override;

            int Update(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) override;

            int Insert(const std::string &table, const std::string &key,
                    std::vector<KVPair> &values) override;

            int Delete(const std::string &table, const std::string &key);
        private:
            MetaDB * db;
    };
}