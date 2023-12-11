#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = std::move(that.bpm_);
  is_dirty_ = std::move(that.is_dirty_);
  page_ = std::move(that.page_);
}

void BasicPageGuard::Drop() {
  assert(is_dirty_ == page_->IsDirty());
  bpm_->UnpinPage(page_->GetPageId(), page_->IsDirty());
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = std::move(that.bpm_);
  this->is_dirty_ = std::move(that.is_dirty_);
  this->page_ = std::move(that.page_);
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  std::cout << "BasicPageGuard destructed" << std::endl;
  Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  guard_.page_->RLatch();
  guard_ = std::move(that.guard_);
  guard_.page_->RUnlatch();
}

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  guard_.page_->RLatch();
  this->guard_ = std::move(that.guard_);
  guard_.page_->RUnlatch();
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.page_->RLatch();
  guard_.Drop();
  guard_.page_->RUnlatch();
}

ReadPageGuard::~ReadPageGuard() {
  std::cout << "ReadPageGuard destructed" << std::endl;
  Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  guard_.page_->WLatch();
  this->guard_ = std::move(that.guard_);
  guard_.page_->WUnlatch();
}

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  guard_.page_->WLatch();
  this->guard_ = std::move(that.guard_);
  guard_.page_->WUnlatch();
  return *this;
}

void WritePageGuard::Drop() {
  guard_.page_->WLatch();
  guard_.Drop();
  guard_.page_->WUnlatch();
}

WritePageGuard::~WritePageGuard() {
  std::cout << "WritePageGuard destructed" << std::endl;
  Drop();
}  // NOLINT

}  // namespace bustub
