//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  // Binds one header page with diskExtendiableHashTable.
  // TODO: The ExtendibleHtableDirectoryPage can be constituted into this class for performance.
  if (header_page_id_ == INVALID_PAGE_ID) {
    auto head_guard = bpm_->NewPageGuarded(&header_page_id_);
    head_guard.AsMut<ExtendibleHTableHeaderPage>()->Init(header_max_depth_);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  assert(header_page_id_ != INVALID_PAGE_ID);

  ReadPageGuard head_guard = bpm_->FetchPageBasic(header_page_id_).UpgradeRead();
  auto *header = head_guard.As<ExtendibleHTableHeaderPage>();

  assert(header != nullptr);

  auto hash = Hash(key);
  auto directory_index = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_index);
  if (directory_page_id == INVALID_PAGE_ID) return false;

  // This page must exists, then can fetch it.
  ReadPageGuard directory_guard = bpm_->FetchPageBasic(directory_page_id).UpgradeRead();
  auto *directory = directory_guard.As<ExtendibleHTableDirectoryPage>();

  assert(directory != nullptr);

  auto bucket_id_index = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_id_index);
  if (bucket_page_id == INVALID_PAGE_ID) return false;

  ReadPageGuard bucket_guard = bpm_->FetchPageBasic(bucket_page_id).UpgradeRead();
  auto *bucket = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  assert(!bucket_guard.IsNull());
  V value;
  auto ret = bucket->Lookup(key, value, cmp_);
  if (!ret) return ret;
  result->emplace_back(std::move(value));
  return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  assert(header_page_id_ != INVALID_PAGE_ID);
  WritePageGuard head_guard = bpm_->FetchPageBasic(header_page_id_).UpgradeWrite();
  auto *header = head_guard.AsMut<ExtendibleHTableHeaderPage>();

  assert(header != nullptr);
  auto hash = Hash(key);

  auto directory_index = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_index);

  WritePageGuard directory_guard;
  if (directory_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directory_index, hash, key, value);
  } else {
    directory_guard = bpm_->FetchPageBasic(directory_page_id).UpgradeWrite();
    assert(!directory_guard.IsNull());
    auto *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
    assert(directory != nullptr);

    // Process bucket.
    auto bucket_id = directory->HashToBucketIndex(hash);
    auto bucket_page_id = directory->GetBucketPageId(bucket_id);
    WritePageGuard bucket_guard{};
    if (bucket_page_id == INVALID_PAGE_ID) {
      bucket_guard = bpm_->NewPageGuarded(&bucket_page_id).UpgradeWrite();
      assert(!bucket_guard.IsNull());

      directory->SetBucketPageId(bucket_id, bucket_page_id);
      return InsertToNewBucket(directory, bucket_id, key, value);
    } else {
      bucket_guard = bpm_->FetchPageBasic(bucket_page_id).UpgradeWrite();
      auto *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      assert(bucket != nullptr);
      if (bucket->IsFull()) {
        // If targetBucket is full and can not extend.
        if (directory->GetLocalDepth(bucket_id) == directory->GetMaxDepth()) return false;

        if (directory->GetLocalDepth(bucket_id) == directory->GetGlobalDepth()) {
          directory->IncrLocalDepth(bucket_id);
          directory->IncrGlobalDepth();
        } else {
          directory->IncrLocalDepth(bucket_id);
          directory->IncrLocalDepth(directory->GetSplitImageIndex(bucket_id));
        }

        page_id_t new_bucket_page_id;
        WritePageGuard new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
        assert(!new_bucket_guard.IsNull());

        auto *new_bucket = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        new_bucket->Init(bucket_max_size_);
        auto new_bucket_id = directory->GetSplitImageIndex(bucket_id);
        auto local_depth_mask = directory->GetLocalDepthMask(bucket_id);
        MigrateEntries(bucket, new_bucket, new_bucket_id, local_depth_mask);
        directory->SetBucketPageId(new_bucket_id, new_bucket_page_id);

        // Insert {key, value} last.
        auto target_bucket_id = directory->HashToBucketIndex(hash);
        if (target_bucket_id == new_bucket_id) {
          return new_bucket->Insert(key, value, cmp_);
        } else {
          return bucket->Insert(key, value, cmp_);
        }
      }
      return bucket->Insert(key, value, cmp_);
    }
  }
}  // namespace bustub

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  K key;
  V value;
  uint32_t target_idx;
  auto oidx = 0, nidx = 0;
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    key = std::move(old_bucket->KeyAt(i));
    value = std::move(old_bucket->ValueAt(i));

    target_idx = Hash(key) & local_depth_mask;
    if (target_idx == new_bucket_idx) {
      new_bucket->PutAt(nidx, std::make_pair(key, value));
      nidx++;
    } else {
      old_bucket->PutAt(oidx, std::make_pair(key, value));
      oidx++;
    }
  }
  new_bucket->SetSize(nidx);
  old_bucket->SetSize(oidx);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directory_page_id;
  WritePageGuard directory_guard = bpm_->NewPageGuarded(&directory_page_id).UpgradeWrite();
  // Allocate new Page.
  assert(!directory_guard.IsNull());
  header->SetDirectoryPageId(directory_idx, directory_page_id);

  auto *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  directory->Init(directory_max_depth_);

  auto bucket_id_index = directory->HashToBucketIndex(hash);
  page_id_t bucket_page_id;
  auto bucket_basic_guard = bpm_->NewPageGuarded(&bucket_page_id);
  assert(!bucket_basic_guard.IsNull());

  bucket_basic_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>()->Init(bucket_max_size_);

  directory->SetBucketPageId(bucket_id_index, bucket_page_id);

  return InsertToNewBucket(directory, bucket_id_index, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  auto bucket_page_id = directory->GetBucketPageId(bucket_idx);
  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  assert(!bucket_guard.IsNull());
  auto *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  return bucket->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  assert(header_page_id_ != INVALID_PAGE_ID);
  auto head_guard = bpm_->FetchPageWrite(header_page_id_);
  auto *header = head_guard.AsMut<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directory_index = header->HashToDirectoryIndex(hash);
  auto directory_page_id = header->GetDirectoryPageId(directory_index);

  if (directory_page_id == INVALID_PAGE_ID) return false;
  WritePageGuard directory_guard = bpm_->FetchPageWrite(directory_page_id);
  assert(directory_guard.IsNull() == false);
  auto *directory = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucket_index = directory->HashToBucketIndex(hash);
  auto bucket_page_id = directory->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) return false;

  WritePageGuard bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  assert(!bucket_guard.IsNull());
  auto *bucket = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  assert(bucket != nullptr);
  auto remove_result = bucket->Remove(key, cmp_);

  if (!remove_result)
    return false;
  else {
    if (bucket->IsEmpty()) {
      if (directory->GetGlobalDepth() == 0) {
        bpm_->UnpinPage(bucket_page_id, false);
        return true;
      }
      directory->DecrLocalDepth(bucket_index);
      auto bucket_y_index = directory->GetSplitImageIndex(bucket_index);
      if (bucket_y_index >= directory->Size()) {
        return false;
      }

      if (bucket_index < bucket_y_index) {
        // Combine {bucketX, bucket_y} = > min(bucketX, bucket_y), whoes bucket's index is less.
        auto bucket_split_image_id = directory->GetBucketPageId(bucket_y_index);
        WritePageGuard bucket_guard_y = bpm_->FetchPageWrite(bucket_split_image_id);
        assert(!bucket_guard_y.IsNull());

        auto *bucket_y = bucket_guard_y.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        assert(bucket_y != nullptr);
        auto local_depth_mask = directory->GetLocalDepthMask(bucket_index);
        MigrateEntries(bucket_y, bucket, bucket_split_image_id, local_depth_mask);
      } else {
        directory->DecrLocalDepth(bucket_y_index);
      }
    }
    while (directory->CanShrink()) {
      directory->DecrGlobalDepth();
    }
    return true;
  }
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
