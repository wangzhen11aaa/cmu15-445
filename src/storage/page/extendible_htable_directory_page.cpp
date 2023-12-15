//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"

namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  max_depth_ = max_depth;
  // 0 <= local_depth_i <= global_depth, 0 <= i < HTABLE_DIRECTORY_ARRAY_SIZE;
  global_depth_ = 0;
  for (uint64_t i = 0; i < static_cast<uint32_t>(0x1 << max_depth); i++) {
    local_depths_[i] = 0;
    bucket_page_ids_[i] = INVALID_PAGE_ID;
  }
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t {
  return hash & GetGlobalDepthMask();
}

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t {
  return bucket_page_ids_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  assert(bucket_idx >= 0 && bucket_idx < (1 << (global_depth_)));
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}
// For {0..1} k-length bits (#2^global_depth),
// When we increase global depth, the next {0...1} k+1 length bits (#2^(global_depth+1)).
// for each new num0', will be the image of num0 of the original.
// For example, 00b's imgage will be 100b.
auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t {
  auto remain_num = (((1 << (global_depth_ - 1)) - 1) & bucket_idx);
  // Special case, when global_depth == 0.
  if (global_depth_ == 0) {
    return bucket_idx ? 0 : 1;
  }
  return (remain_num | ((bucket_idx & (1 << (global_depth_ - 1))) ? 0 : (1 << (global_depth_ - 1))));
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { return global_depth_; }

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  ++global_depth_;
  // Increment the directory, and use each image of original number to fill the pages and local_depth.
  int delta = 1 << (global_depth_ - 1);
  for (auto newIndex = 1 << (global_depth_ - 1); newIndex < (1 << global_depth_); newIndex++) {
    auto origin_bucket_index = newIndex - delta;
    bucket_page_ids_[newIndex] = bucket_page_ids_[origin_bucket_index];
    local_depths_[newIndex] = local_depths_[origin_bucket_index];
  }
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() { --global_depth_; }

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool {
  bool canShrink = true;
  for (auto i = 0; i < (1 << global_depth_); i++) {
    if (local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return canShrink;
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t { return static_cast<uint32_t>(0x1 << global_depth_); }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t {
  return local_depths_[bucket_idx];
}

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]++; }

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { local_depths_[bucket_idx]--; }

}  // namespace bustub
