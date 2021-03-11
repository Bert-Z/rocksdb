#include "third-party/Rosetta/include/new/elastic_rosetta.hpp"

#include <algorithm>
#include <iostream>

#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"
#include "table/block_based_filter_block.h"
#include "table/full_filter_bits_builder.h"
#include "table/full_filter_block.h"

namespace rocksdb {

class BlockBasedFilterBlockBuilder;
class FullFilterBlockBuilder;

class FullElasticRosettaBitsBuilder : public FilterBitsBuilder {
 public:
  explicit FullElasticRosettaBitsBuilder(
      uint64_t bits_per_key, std::vector<uint64_t> last_level_bits_per_keys)
      : bits_per_key_(bits_per_key),
        last_level_bits_per_keys_(last_level_bits_per_keys){};

  ~FullElasticRosettaBitsBuilder(){};

  virtual void AddKey(const Slice& key) override {
    levels_ = std::max(key.size() * 8, levels_);
    keys_.push_back(DecodeFixed64(key.data()));
  }

  virtual int CalculateNumEntry(const uint32_t space) override {
    return (int)keys_.size();
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    size_t keys_size = keys_.size();

    elastic_rose::Elastic_Rosetta* filter = new elastic_rose::Elastic_Rosetta(
        keys_, bits_per_key_, last_level_bits_per_keys_);

    uint64_t size = filter->serializedSize();
    char* data = filter->serialize();
    delete filter;

    buf->reset(data);
    return Slice(data, size);
  }

 private:
  uint64_t levels_ = 64;
  uint64_t bits_per_key_;
  std::vector<uint64_t> last_level_bits_per_keys_;
  std::vector<uint64_t> keys_;
};

class FullElasticRosettaBitsReader : public FilterBitsReader {
 public:
  explicit FullElasticRosettaBitsReader(const Slice& contents,
                                        uint32_t opensize)
      : data_(const_cast<char*>(contents.data())),
        size_(static_cast<uint64_t>(contents.size())),
        opensize_(opensize) {
    char* data = data_;
    filter_ = elastic_rose::Elastic_Rosetta::deSerialize(data, opensize);
  }

  ~FullElasticRosettaBitsReader() { delete filter_; }

  virtual bool MayMatch(const Slice& entry) override {
    uint64_t key = DecodeFixed64(entry.data());
    return filter_->lookupKey(key);
  }

  // wanqiang
  virtual Slice Seek(const Slice& entry, unsigned* bitlen) override {
    uint64_t key = DecodeFixed64(entry.data());
    uint64_t result = filter_->seek(key);
    result = htobe64(result);
    return Slice(reinterpret_cast<const char*>(&result), sizeof(result));
  };

 private:
  char* data_;
  uint64_t size_;
  uint32_t opensize_;
  elastic_rose::Elastic_Rosetta* filter_;
};

class ElasticRosettaPolicy : public FilterPolicy {
 public:
  explicit ElasticRosettaPolicy(bool use_block_based_builder,
                                uint64_t bits_per_key,
                                std::vector<uint64_t> last_level_bits_per_keys)
      : use_block_based_builder_(use_block_based_builder),
        bits_per_key_(bits_per_key),
        last_level_bits_per_keys_(last_level_bits_per_keys){};

  ~ElasticRosettaPolicy(){};

  virtual const char* Name() const override { return "rocksdb.ElasticRosettaFilter"; }

  virtual void CreateFilter(const Slice* keys, int n,
                            std::string* dst) const override {
    std::vector<uint64_t> keys_str;
    for (size_t i = 0; i < (size_t)n; i++) {
      keys_str.push_back(DecodeFixed64(keys[i].data()));
    }

    elastic_rose::Elastic_Rosetta* filter = new elastic_rose::Elastic_Rosetta(
        keys_str, bits_per_key_, last_level_bits_per_keys_);

    uint64_t size = filter->serializedSize();
    char* data = filter->serialize();
    dst->append(data, size);

    delete filter;
  }

  virtual bool KeyMayMatch(const Slice& entry,
                           const Slice& filter) const override {
    return true;
  }

  virtual bool ElasticKeyMayMatch(const Slice& entry, const Slice& filter,
                                  uint32_t opensize) const override {
    char* filter_data = const_cast<char*>(filter.data());
    char* data = filter_data;
    elastic_rose::Elastic_Rosetta* filter_elastic_rosetta =
        elastic_rose::Elastic_Rosetta::deSerialize(data, opensize);

    uint64_t key = DecodeFixed64(entry.data());
    bool found = filter_elastic_rosetta->lookupKey(key);
    delete filter_elastic_rosetta;
    return found;
  }

  virtual FilterBitsBuilder* GetFilterBitsBuilder() const override {
    if (use_block_based_builder_) {
      return nullptr;
    }
    return new FullElasticRosettaBitsBuilder(bits_per_key_,
                                             last_level_bits_per_keys_);
  }

  // duplicated
  virtual FilterBitsReader* GetFilterBitsReader(
      const Slice& contents) const override {
    return new FullElasticRosettaBitsReader(contents, 3);
  }

  virtual FilterBitsReader* ElasticGetFilterBitsReader(
      const Slice& contents, uint32_t opensize) const override {
    return new FullElasticRosettaBitsReader(contents, opensize);
  }

 private:
  const bool use_block_based_builder_;
  uint64_t bits_per_key_;
  std::vector<uint64_t> last_level_bits_per_keys_;
};

const FilterPolicy* NewElasticRosettaPolicy(
    bool use_block_based_builder, uint64_t bits_per_key,
    std::vector<uint64_t> last_level_bits_per_keys) {
  return new ElasticRosettaPolicy(use_block_based_builder, bits_per_key,
                                  last_level_bits_per_keys);
}

}  // namespace rocksdb