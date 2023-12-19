#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  bpm_ = std::move(that.bpm_);
  is_dirty_ = std::move(that.is_dirty_);
  page_ = std::move(that.page_);

  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  that.page_ = nullptr;
}

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  assert(page_ != nullptr);
  page_->RLatch();
  return ReadPageGuard(this->bpm_, this->page_);
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  assert(page_ != nullptr);
  page_->WLatch();
  return WritePageGuard(this->bpm_, this->page_);
}

void BasicPageGuard::Drop() {
  BUSTUB_ASSERT(page_ != nullptr, "Drop page with nullptr value");
  bpm_->UnpinPage(page_->GetPageId(), page_->IsDirty());
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  this->bpm_ = std::move(that.bpm_);
  this->is_dirty_ = std::move(that.is_dirty_);
  this->page_ = std::move(that.page_);

  that.bpm_ = nullptr;
  that.is_dirty_ = false;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() {
  // std::cout << "BasicPageGuard destructed" << std::endl;
  if (page_ != nullptr) {
    Drop();
  }
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  guard_.Drop();
  if (guard_.page_->GetPinCount() <= 0) {
    guard_.page_->RUnlatch();
    guard_.page_ = nullptr;
  }
}

ReadPageGuard::~ReadPageGuard() {
  if (guard_.page_ != nullptr) {
    Drop();
  }
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  guard_.Drop();
  if (guard_.page_->GetPinCount() <= 0) {
    guard_.page_->WUnlatch();
    guard_.page_ = nullptr;
  }
}

WritePageGuard::~WritePageGuard() {
  // std::cout << "WritePageGuard destructed" << std::endl;
  if (guard_.page_ != nullptr) {
    // std::cout << "WritePageGuard destructed, page_ " << guard_.PageId() << " Released" << std::endl;
    Drop();
  }
}  // NOLINT

}  // namespace bustub
