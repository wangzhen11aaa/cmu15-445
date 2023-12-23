#include "concurrency/watermark.h"
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  // TODO(fall2023): implement me!

  if (current_reads_.find(read_ts) != current_reads_.end()) {
    current_reads_[read_ts]++;
  } else {
    current_reads_[read_ts] = 1;
    min_heap_.push(read_ts);
  }
  watermark_ = min_heap_.top();
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!

  current_reads_[read_ts]--;
  if (current_reads_[read_ts] == 0) {
    auto ts = min_heap_.top();
    if (read_ts == ts) {
      min_heap_.pop();
      watermark_ = min_heap_.top();
    }
    current_reads_.erase(read_ts);
  }
}

}  // namespace bustub
