#include "third-party/Rosetta/include/new/rosetta.hpp"

#include <algorithm>
#include <iostream>

#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"
#include "table/block_based_filter_block.h"
#include "table/full_filter_bits_builder.h"
#include "table/full_filter_block.h"

namespace rocksdb {

std::string str2BitArray(const std::string& str, uint32_t levels) {
  std::string ret = "";
  for (auto c : str)
    for (int i = 7; i >= 0; --i) ret += (((c >> i) & 1) ? '1' : '0');

  // format str size
  int diff = (int)levels - (int)ret.size();
  assert(diff >= 0);
  if (diff >= 0) ret = std::string(diff, '0') + ret;

  return ret;
}

class BlockBasedFilterBlockBuilder;
class FullFilterBlockBuilder;

class FullRosettaBitsBuilder : public FilterBitsBuilder {
 public:
  explicit FullRosettaBitsBuilder(uint64_t bits_per_key)
      : bits_per_key_(bits_per_key){};

  ~FullRosettaBitsBuilder(){};

  virtual void AddKey(const Slice& key) override {
    levels_ = std::max(key.size() * 8, levels_);
    // keys_.push_back( std::string(key.data(),8));
    keys_.push_back(DecodeFixed64(key.data()));
  }

  virtual int CalculateNumEntry(const uint32_t space) override {
    return (int)keys_.size();
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override {
    size_t keys_size = keys_.size();

    // format key to bitarray
    // for (size_t i = 0; i < keys_size; ++i)
    //   keys_[i] = str2BitArray(keys_[i], levels_);

    // elastic_rose::Rosetta* filter =
    //     new elastic_rose::Rosetta(keys_, keys_size, levels_, bits_per_key_);

    elastic_rose::Rosetta* filter =
        new elastic_rose::Rosetta(keys_, keys_size, bits_per_key_);

    uint64_t size = filter->serializedSize();
    char* data = filter->serialize();
    delete filter;

    buf->reset(data);
    return Slice(data, size);
  }

 private:
  uint64_t levels_ = 64;
  uint64_t bits_per_key_;
  // std::vector<std::string> keys_;
  std::vector<uint64_t> keys_;
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
    // format key to bitarray
    // std::string key =
    //     str2BitArray(std::string(entry.data(), 8), filter_->getLevels());

    uint64_t key = DecodeFixed64(entry.data());

    return filter_->lookupKey(key);
  }

  // wanqiang
  virtual Slice Seek(const Slice& entry, unsigned* bitlen) override {
    // format key to bitarray
    // std::string key = str2BitArray(std::string(entry.data(), 8),
    // filter_->getLevels()); return Slice(filter_->seek(key));
    uint64_t key = DecodeFixed64(entry.data());
    uint64_t result = filter_->seek(key);
    result = htobe64(result);
    return Slice(reinterpret_cast<const char*>(&result), sizeof(result));
  };

 private:
  char* data_;
  uint64_t size_;
  elastic_rose::Rosetta* filter_;
};

class RosettaPolicy : public FilterPolicy {
 public:
  explicit RosettaPolicy(bool use_block_based_builder, uint64_t bits_per_key)
      : use_block_based_builder_(use_block_based_builder),
        bits_per_key_(bits_per_key){};

  ~RosettaPolicy(){};

  virtual const char* Name() const override { return "rocksdb.RosettaFilter"; }

  virtual void CreateFilter(const Slice* keys, int n,
                            std::string* dst) const override {
    // std::vector<std::string> keys_str;
    // uint64_t levels_ = 64;
    // for (size_t i = 0; i < (size_t)n; i++) {
    //   levels_ = std::max(levels_, keys_str.size());
    //   keys_str.push_back(std::string(keys[i].data(), 8));
    // }

    // elastic_rose::Rosetta* filter = new elastic_rose::Rosetta(
    //     keys_str, keys_str.size(), levels_, bits_per_key_);

    std::vector<uint64_t> keys_str;
    for (size_t i = 0; i < (size_t)n; i++) {
      keys_str.push_back(DecodeFixed64(keys[i].data()));
    }

    elastic_rose::Rosetta* filter =
        new elastic_rose::Rosetta(keys_str, keys_str.size(), bits_per_key_);

    uint64_t size = filter->serializedSize();
    char* data = filter->serialize();
    dst->append(data, size);

    delete filter;
  }

  virtual bool KeyMayMatch(const Slice& entry,
                           const Slice& filter) const override {
    char* filter_data = const_cast<char*>(filter.data());
    char* data = filter_data;
    elastic_rose::Rosetta* filter_rosetta =
        elastic_rose::Rosetta::deSerialize(data);
    // format key to bitarray
    // std::string key =
    //     str2BitArray(std::string(entry.data(), 8),
    //     filter_rosetta->getLevels());
    uint64_t key = DecodeFixed64(entry.data());
    bool found = filter_rosetta->lookupKey(key);
    delete filter_rosetta;
    return found;
  }

  virtual FilterBitsBuilder* GetFilterBitsBuilder() const override {
    if (use_block_based_builder_) {
      return nullptr;
    }
    return new FullRosettaBitsBuilder(bits_per_key_);
  }

  virtual FilterBitsReader* GetFilterBitsReader(
      const Slice& contents) const override {
    return new FullRosettaBitsReader(contents);
  }

 private:
  const bool use_block_based_builder_;
  uint64_t bits_per_key_;
};

const FilterPolicy* NewRosettaPolicy(bool use_block_based_builder,
                                     uint64_t bits_per_key) {
  return new RosettaPolicy(use_block_based_builder, bits_per_key);
}

}  // namespace rocksdb