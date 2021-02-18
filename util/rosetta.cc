#include "third-party/Rosetta/include/rosetta.hpp"

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

class FullRosettaBitsBuilder : public FilterBitsBuilder {
 public:
  explicit FullRosettaBitsBuilder(){};

  ~FullRosettaBitsBuilder(){};

  virtual void AddKey(const Slice& key) override {
    levels_ = std::max(key.size() * 8, levels_);
    keys_.push_back(std::string(key.data(), key.size()));
  }

  virtual int CalculateNumEntry(const uint32_t space) override {
    return (int)keys_.size();
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    elastic_rose::Rosetta* filter =
        new elastic_rose::Rosetta(keys_, keys_.size(), levels_);

    uint64_t size = filter->serializedSize();
    char* data = filter->serialize();

    buf->reset(data);
    return Slice(data, size);
  }

 private:
  uint64_t levels_ = 64;
  std::vector<std::string> keys_;
};

class FullRosettaBitsReader : public FilterBitsReader {
 public:
  explicit FullRosettaBitsReader(const Slice& contents)
      : data_(const_cast<char*>(contents.data())),
        size_(static_cast<uint64_t>(contents.size())) {
    char* data = data_;
    filter_ = elastic_rose::Rosetta::deSerialize(data);
  }

  ~FullRosettaBitsReader() { delete filter_; }

  virtual bool MayMatch(const Slice& entry) override {
    return filter_->lookupKey(std::string(entry.data(), entry.size()));
  }

 private:
  char* data_;
  uint64_t size_;
  elastic_rose::Rosetta* filter_;
};

class RosettaPolicy : public FilterPolicy {
 public:
  explicit RosettaPolicy(bool use_block_based_builder)
      : use_block_based_builder_(use_block_based_builder){};

  ~RosettaPolicy(){};

  virtual const char* Name() const override { return "rocksdb.RosettaFilter"; }

  virtual void CreateFilter(const Slice* keys, int n,
                            std::string* dst) const override {
    std::vector<std::string> keys_str;
    uint64_t levels_ = 64;
    for (size_t i = 0; i < (size_t)n; i++) {
      levels_ = std::max(levels_, keys_str.size());
      keys_str.push_back(std::string(keys[i].data(), keys[i].size()));
    }

    elastic_rose::Rosetta* filter =
        new elastic_rose::Rosetta(keys_str, keys_str.size(), levels_);

    uint64_t size = filter->serializedSize();
    char* data = filter->serialize();
    dst->append(data, size);

    delete filter;
  }

  virtual bool KeyMayMatch(const Slice& key,
                           const Slice& filter) const override {
    char* filter_data = const_cast<char*>(filter.data());
    char* data = filter_data;
    elastic_rose::Rosetta* filter_rosetta =
        elastic_rose::Rosetta::deSerialize(data);
    bool found = filter_rosetta->lookupKey(std::string(key.data(), key.size()));
    delete filter_rosetta;
    return found;
  }

  virtual FilterBitsBuilder* GetFilterBitsBuilder() const override {
    if (use_block_based_builder_) {
      return nullptr;
    }
    return new FullRosettaBitsBuilder();
  }

  virtual FilterBitsReader* GetFilterBitsReader(
      const Slice& contents) const override {
    return new FullRosettaBitsReader(contents);
  }

 private:
  const bool use_block_based_builder_;
};

const FilterPolicy* NewRosettaPolicy(bool use_block_based_builder) {
  return new RosettaPolicy(use_block_based_builder);
}

}  // namespace rocksdb
