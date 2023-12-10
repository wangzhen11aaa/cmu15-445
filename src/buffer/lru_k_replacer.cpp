//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  // Iterate all LRUKReplacer's map to find the one node, which maximum backwards distance and minumum timestamap.
  bool found = false;
  // assert(node_store_.size() >= replacer_size_);
  // If no node's status is evicable.
  if (curr_size_ < 1) {
    return false;
  }
  // max_distance set to k_+1.
  unsigned int max_distance = 0;
  size_t min_ts = UINT64_MAX;
  size_t curDistance, curMinTimestamp;
  // Constant time iteration.
  for (auto &kv : node_store_) {
    // If node is not evictable, continue;
    if (!kv.second.is_evictable_) {
      continue;
    }
    curDistance = NodeDistance(kv.second);

    if (max_distance <= curDistance) {
      if (max_distance < curDistance) {
        found = true;
        *frame_id = kv.first;
        max_distance = curDistance;
        min_ts = NodeMinimumTimestamp(kv.second);
        if (min_ts == 0) {
          break;
        }
      } else {
        curMinTimestamp = NodeMinimumTimestamp(kv.second);
        if (min_ts > curMinTimestamp) {
          found = true;
          *frame_id = kv.first;
          min_ts = curMinTimestamp;
        }
      }
    }
  }
  if (found) {
    curr_size_--;
    node_store_[*frame_id].is_evictable_ = false;
    node_store_[*frame_id].history_.clear();
    node_store_[*frame_id].k_ = 0;
  }
  return found;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) noexcept {
  // access_type now unused.
  // 0 =< frame_id  < replacer_size
  assert(frame_id < (int)replacer_size_);
  assert(frame_id != INVALID_PAGE_ID);
  // Write operation: _latch the core data.
  latch_.lock();

  // If frame_id has been pinned in the memory buffer.
  if (node_store_.count(frame_id)) {
    // LRUKNode &node = node_store_[frame_id];
    assert(node_store_[frame_id].fid_ == frame_id);

    node_store_[frame_id].history_.insert(node_store_[frame_id].history_.begin(), std::move(GetCurrentTimestamp()));

    // If lruNode's length (k_) >= node_store_.k, remove last from the list.
    if (node_store_[frame_id].k_ >= k_) {
      node_store_[frame_id].history_.pop_back();
    } else {
      node_store_[frame_id].k_++;
    }
    latch_.unlock();
  } else {
    node_store_[frame_id] = LRUKNode{GetCurrentTimestamp(), frame_id};
    latch_.unlock();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  //   std::lock_guard<std::mutex> lock(latch_);
  // Make sure frame_id is valid.
  assert(frame_id < (int)replacer_size_);
  assert(node_store_.count(frame_id) > 0);
  if (node_store_[frame_id].is_evictable_ != set_evictable) {
    if (set_evictable) {
      ++curr_size_;
      node_store_[frame_id].is_evictable_ = set_evictable;
    } else {
      --curr_size_;
      node_store_[frame_id].is_evictable_ = set_evictable;
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  assert(node_store_[frame_id].is_evictable_ == true);
  // Value is object,when erased, its destructor is called.
  node_store_.erase(frame_id);
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

LRUKReplacer::~LRUKReplacer() { std::cout << "LRUKReplacer is deleted \n"; }

}  // namespace bustub
