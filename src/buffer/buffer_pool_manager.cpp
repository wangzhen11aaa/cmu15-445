//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  *page_id = AllocatePage();

  latch_.lock();
  if (free_list_.empty() && replacer_->Size() == 0) {
    latch_.unlock();
    return nullptr;
  }
  frame_id_t freePageFrameId;
  auto emptyPagePtr = FetchEmptyPageFrameFromFL(freePageFrameId);
  if (emptyPagePtr) {
    SetUpPage(pages_[freePageFrameId], *page_id, freePageFrameId);
    latch_.unlock();
    return emptyPagePtr;
  } else {
    emptyPagePtr = FetchEmptyPageFrameViaEvict(&freePageFrameId);
    // Write Back page if dirty, synchronously.
    if (emptyPagePtr->IsDirty()) {
      WritePageBack(*emptyPagePtr);
      emptyPagePtr->is_dirty_ = false;
    }
    ReUsePage(pages_[freePageFrameId], *page_id, freePageFrameId);
    latch_.unlock();
    return emptyPagePtr;
  }
}

auto BufferPoolManager::FetchEmptyPageFrameViaEvict(frame_id_t *freePageFrameId) -> Page * {
  // Select one unpinned page to use.
  bool evitFlag = replacer_->Evict(freePageFrameId);
  assert(evitFlag == true);
  // Remove old mapping.
  auto pageId = pages_[*freePageFrameId].GetPageId();
  page_table_.erase(pageId);
  return pages_ + *freePageFrameId;
}

auto BufferPoolManager::FetchEmptyPageFrameFromFL(frame_id_t &freePageFrameId) -> Page * {
  if (free_list_.empty()) {
    return nullptr;
  }
  freePageFrameId = free_list_.front();
  free_list_.pop_front();

  assert(page_table_.count(freePageFrameId) == 0);
  return pages_ + freePageFrameId;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // If it is in the page_table_, return directly.
  std::lock_guard<std::mutex> lock(latch_);
  // Exists one page_frame_id mapped with page_id, not in buffer pool now.
  if (page_table_.count(page_id)) {
    auto pageFrameToUse = page_table_[page_id];
    // It is safe to refer to this page frame.
    pages_[pageFrameToUse].pin_count_++;
    return pages_ + page_id;
  } else {
    // Get a empty page frame to use, maybe evict page.
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }
    frame_id_t freePageFrameId;
    // Find one empty PageFrame. Can not use NewPage method.
    auto emptyPagePtr = FetchEmptyPageFrameFromFL(freePageFrameId);
    if (emptyPagePtr) {
      SetUpPage(pages_[freePageFrameId], page_id, freePageFrameId);
      return emptyPagePtr;
    } else {
      emptyPagePtr = FetchEmptyPageFrameViaEvict(&freePageFrameId);

      // Write Back page if dirty, synchronously.
      if (emptyPagePtr->IsDirty()) {
        WritePageBack(*emptyPagePtr);
        emptyPagePtr->is_dirty_ = false;
      }

      ReUsePage(pages_[freePageFrameId], page_id, freePageFrameId);
      ReadPage(*emptyPagePtr);
      return emptyPagePtr;
    }
  }
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  if (!page_table_.count(page_id)) {
    return false;
  }
  auto pageframeIdToUnPin = page_table_[page_id];
  pages_[pageframeIdToUnPin].is_dirty_ = is_dirty;

  if (pages_[pageframeIdToUnPin].pin_count_ <= 0) return false;
  if (--pages_[pageframeIdToUnPin].pin_count_ <= 0) {
    replacer_->SetEvictable(pageframeIdToUnPin, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  assert(page_id != INVALID_PAGE_ID);
  latch_.lock();
  if (!page_table_.count(page_id)) {
    latch_.unlock();
    return false;
  }
  auto pageFrameIdToFlush = page_table_[page_id];
  pages_[pageFrameIdToFlush].pin_count_++;
  latch_.unlock();
  WritePageBack(pages_[pageFrameIdToFlush]);
  pages_[pageFrameIdToFlush].is_dirty_ = false;

  std::lock_guard<std::mutex> lock(latch_);
  if (--pages_[pageFrameIdToFlush].pin_count_ == 0) {
    replacer_->SetEvictable(pageFrameIdToFlush, true);
  }
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // std::lock_guard<std::mutex> lock(latch_);
  latch_.lock();
  std::vector<page_id_t> pageIds(page_table_.size());
  auto i = 0;
  for (auto &kv : page_table_) {
    pageIds[i++] = kv.first;
  }
  latch_.unlock();
  for (auto &pageId : pageIds) {
    FlushPage(pageId);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();
  if (!page_table_.count(page_id)) {
    latch_.unlock();
    return true;
  }
  auto pageframeIdToDelete = page_table_[page_id];
  if (pages_[pageframeIdToDelete].pin_count_ > 0) {
    return false;
  }
  page_table_.erase(page_id);
  replacer_->node_store_.erase(pageframeIdToDelete);
  ResetPage(pages_[pageframeIdToDelete]);
  // Insert into free_list.
  free_list_.push_back(pageframeIdToDelete);
  latch_.unlock();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t {
  std::lock_guard<std::mutex> lock(latch_);
  return next_page_id_++;
}

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  auto pagePtr = FetchPage(page_id);
  return {this, pagePtr};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto pagePtr = FetchPage(page_id);
  pagePtr->RLatch();
  return {this, pagePtr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto pagePtr = FetchPage(page_id);
  pagePtr->pin_count_ = 1;
  pagePtr->WLatch();
  return {this, pagePtr};
  // return {this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  auto page = NewPage(page_id);
  if (!page) {
    return {this, nullptr};
  } else {
    return {this, page};
  }
}

}  // namespace bustub
