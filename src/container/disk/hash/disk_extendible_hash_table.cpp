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
    bpm_->NewPageGuarded(&header_page_id_);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  assert(header_page_id_ != INVALID_PAGE_ID);

  BasicPageGuard headerGuard = bpm_->FetchPageBasic(header_page_id_);
  auto *header = headerGuard.As<ExtendibleHTableHeaderPage>();

  assert(header != nullptr);

  auto hash = Hash(key);
  auto directoryIdIndex = header->HashToDirectoryIndex(hash);
  auto directoryPageId = header->GetDirectoryPageId(directoryIdIndex);
  if (directoryPageId == INVALID_PAGE_ID) return false;

  // This page must exists, then can fetch it.
  BasicPageGuard directoryGuard = bpm_->FetchPageBasic(directoryPageId);
  auto *directory = directoryGuard.As<ExtendibleHTableDirectoryPage>();

  assert(directory != nullptr);

  auto bucketIdIndex = directory->HashToBucketIndex(hash);
  auto bucketPageId = directory->GetBucketPageId(bucketIdIndex);
  if (bucketPageId == INVALID_PAGE_ID) return false;

  BasicPageGuard bucketGuard = bpm_->FetchPageBasic(bucketPageId);
  auto *bucket = bucketGuard.As<ExtendibleHTableBucketPage<K, V, KC>>();

  assert(!bucketGuard.IsNull());
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
  BasicPageGuard headerGuard = bpm_->FetchPageBasic(header_page_id_);
  auto *header = headerGuard.AsMut<ExtendibleHTableHeaderPage>();

  assert(header != nullptr);
  auto hash = Hash(key);

  auto directoryIdIndex = header->HashToDirectoryIndex(hash);
  auto directoryPageId = header->GetDirectoryPageId(directoryIdIndex);

  BasicPageGuard directoryGuard;
  if (directoryPageId == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header, directoryIdIndex, hash, key, value);
  } else {
    directoryGuard = bpm_->FetchPageBasic(directoryPageId);
    assert(!directoryGuard.IsNull());
    auto *directory = directoryGuard.AsMut<ExtendibleHTableDirectoryPage>();
    assert(directory != nullptr);

    // Process bucket.
    auto bucketId = directory->HashToBucketIndex(hash);
    auto bucketPageId = directory->GetBucketPageId(bucketId);
    BasicPageGuard bucketGuard{};
    if (bucketPageId == INVALID_PAGE_ID) {
      bucketGuard = std::move(bpm_->NewPageGuarded(&bucketPageId));
      assert(!bucketGuard.IsNull());

      directory->SetBucketPageId(bucketId, bucketPageId);
      return InsertToNewBucket(directory, bucketId, key, value);
    } else {
      bucketGuard = bpm_->FetchPageBasic(bucketPageId);
      auto *bucket = bucketGuard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
      assert(bucket != nullptr);
      if (bucket->IsFull()) {
        directory->IncrLocalDepth(bucketId);
        directory->IncrGlobalDepth();

        page_id_t nBucketPageId;
        BasicPageGuard nBucketGuard = bpm_->NewPageGuarded(&nBucketPageId);
        assert(!nBucketGuard.IsNull());

        auto *nBucket = nBucketGuard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        auto nBucketId = directory->GetSplitImageIndex(bucketId);
        auto localDepthMask = directory->GetLocalDepthMask(bucketId);
        MigrateEntries(bucket, nBucket, nBucketId, localDepthMask);
        directory->SetBucketPageId(nBucketId, nBucketPageId);
      }
      return bucket->Insert(key, value, cmp_);
    }
  }
}
template <typename K, typename V, typename KC>
void MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket, ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                    uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  K key;
  V value;
  uint32_t targetIdx;
  auto oidx = 0, nidx = 0;
  for (auto i = 0; i < old_bucket->Size(); i++) {
    key = std::move(old_bucket->KeyAt(i));
    value = std::move(old_bucket->ValueAt(i));

    targetIdx = Hash(key) & local_depth_mask;
    if (targetIdx == new_bucket_idx) {
      new_bucket->EntryAt(nidx++) = std::move({key, value});
    } else {
      old_bucket->EntryAt(oidx++) = std::move({key, value});
    }
  }
  new_bucket->setSize(nidx);
  old_bucket->SetSize(oidx);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t directoryPageId;
  BasicPageGuard directoryGuard;
  directoryGuard = bpm_->NewPageGuarded(&directoryPageId);
  // Allocate new Page.
  assert(!directoryGuard.IsNull());
  header->SetDirectoryPageId(directory_idx, directoryPageId);

  auto *directory = directoryGuard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucketIdIndex = directory->HashToBucketIndex(hash);
  page_id_t bucketPageId;
  BasicPageGuard bucketGuard;
  bucketGuard = bpm_->NewPageGuarded(&bucketPageId);
  assert(!bucketGuard.IsNull());

  directory->SetBucketPageId(bucketIdIndex, bucketPageId);

  InsertToNewBucket(directory, bucketIdIndex, key, value);
  return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  BasicPageGuard bucketGuard = bpm_->FetchPageBasic(directory->GetBucketPageId(bucket_idx));
  assert(!bucketGuard.IsNull());
  auto *bucket = bucketGuard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
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
  BasicPageGuard headerGuard = bpm_->FetchPageBasic(header_page_id_);
  auto *header = headerGuard.AsMut<ExtendibleHTableHeaderPage>();

  auto hash = Hash(key);
  auto directoryIdx = header->HashToDirectoryIndex(hash);
  auto directoryPageId = header->GetDirectoryPageId(directoryIdx);

  if (directoryPageId == INVALID_PAGE_ID) return false;
  BasicPageGuard directoryGuard = bpm_->FetchPageBasic(directoryPageId);
  assert(directoryGuard.IsNull() == false);
  auto *directory = directoryGuard.AsMut<ExtendibleHTableDirectoryPage>();

  auto bucketIndex = directory->HashToBucketIndex(hash);
  auto bucketPageId = directory->GetBucketPageId(bucketIndex);
  if (bucketPageId == INVALID_PAGE_ID) return false;

  BasicPageGuard bucketGuard = bpm_->FetchPageBasic(bucketPageId);
  assert(!bucketGuard.IsNull());
  auto *bucket = bucketGuard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  assert(bucket != nullptr);
  auto removeResult = bucket->Remove(key, cmp_);

  if (!removeResult)
    return false;
  else {
    if (bucket->IsEmpty()) {
      directory->DecrLocalDepth(bucketIndex);
      auto bucketYIndex = directory->GetSplitImageIndex(bucketIndex);
      if (bucketIndex < bucketYIndex) {
        // Combine {bucketX, bucketY} = > min(bucketX, bucketY), whoes bucket's index is less.
        auto bucketSplitImageId = directory->GetBucketPageId(bucketYIndex);
        BasicPageGuard bucketGuardY = bpm_->FetchPageBasic(bucketSplitImageId);
        assert(!bucketGuardY.IsNull());

        auto *bucketY = bucketGuardY.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
        assert(bucketY != nullptr);
        auto localDepthMask = directory->GetLocalDepthMask(bucketIndex);
        MigrateEntries(bucketY, bucket, bucketSplitImageId, localDepthMask);
      } else {
        directory->DecrLocalDepth(bucketYIndex);
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
