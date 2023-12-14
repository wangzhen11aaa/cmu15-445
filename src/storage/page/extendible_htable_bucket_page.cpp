//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  size_ = 0;
  max_size_ = max_size;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  uint32_t left = 0, right = size_, mid;
  while (left < right) {
    mid = left + ((right - left) >> 1);
    if (cmp(array_[mid].first, key) == 0) {
      value = array_[mid].second;
      return true;
    } else if (cmp(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return false;
}
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {
  if (size_ == max_size_) return false;

  uint32_t left = 0, right = size_, mid;
  while (left < right) {
    mid = left + ((right - left) >> 1);
    if (cmp(array_[mid].first, key) == 0) {
      left = mid;
      break;
    } else if (cmp(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  if (left < size_ && cmp(array_[left].first, key) == 0) return false;
  // Move [...,left,..., size_-1] right forward 1 step.
  for (auto pos = size_; pos > left; pos--) {
    array_[pos] = std::move(array_[pos - 1]);
  }
  array_[left] = std::move(std::make_pair(key, value));
  size_++;
  return true;
}
template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  uint32_t left = 0, right = size_, mid;
  while (left < right) {
    mid = left + ((right - left) >> 1);
    if (cmp(array_[mid].first, key) == 0) {
      left = mid;
      break;
    } else if (cmp(array_[mid].first, key) < 0) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  if (cmp(array_[left].first, key) != 0) return false;
  // Move [...,left,..., size_-1] right forward 1 step.
  for (auto pos = left; pos < size_; pos++) {
    array_[pos] = std::move(array_[pos + 1]);
  }
  size_--;
  return true;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
  assert(bucket_idx >= 0 && bucket_idx < size_);
  for (auto pos = bucket_idx; pos < size_; pos++) {
    array_[pos] = std::move(array_[pos + 1]);
  }
  size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
  assert(bucket_idx >= 0 && bucket_idx < size_);
  return array_[bucket_idx].first;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
  assert(bucket_idx >= 0 && bucket_idx < size_);
  return array_[bucket_idx].second;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  assert(bucket_idx >= 0 && bucket_idx < size_);
  return array_[bucket_idx];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
  return size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  return size_ == max_size_;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  return size_ == 0;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::PutAt(int index, std::pair<K, V> &&p) {}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
