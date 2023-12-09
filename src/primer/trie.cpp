#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
using std::make_shared;
using std::shared_ptr;
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (!root_) return nullptr;

  if (key.empty()) {
    if (root_->is_value_node_) {
      return dynamic_cast<const TrieNodeWithValue<T> *>(root_.get())->value_.get();
    }
    return nullptr;
  }

  auto cursorRoot = root_;

  if (cursorRoot->children_.empty()) return nullptr;

  for (const auto &c : key) {
    if (cursorRoot->children_.count(c) == 0)
      return nullptr;
    else {
      cursorRoot = cursorRoot->children_.at(c);
    }
  }
  auto res = dynamic_cast<const TrieNodeWithValue<T> *>(cursorRoot.get());
  return res != nullptr ? res->value_.get() : nullptr;
}

// Non-recurive version.
template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.

  // If Trie is empty;
  shared_ptr<TrieNode> newRoot{};

  if (root_ == nullptr) {
    if (key.empty()) {
      shared_ptr<TrieNode> newRootWithV = std::make_shared<TrieNodeWithValue<T>>(make_shared<T>(std::move(value)));
      return Trie{newRootWithV};
    }
    newRoot = make_shared<TrieNode>();
  } else {
    // Convert TrieNode into TrieNodeWithValue
    if (key.empty()) {
      shared_ptr<TrieNode> newRootWithV =
          std::make_shared<TrieNodeWithValue<T>>(std::move(root_->children_), make_shared<T>(std::move(value)));
      return Trie{newRootWithV};
    }
    // newRoot = std::make_shared<TrieNode>(std::move(root_->Clone()->children_));
    newRoot = shared_ptr<TrieNode>{root_->Clone()};
  }

  shared_ptr<TrieNode> cursorRoot{};
  decltype(cursorRoot) parent = newRoot;

  // Copy-On-Write, Create a lot of nodes until met the last position of key.
  char curIndex;
  for (auto it = key.begin(); it != key.end() - 1; ++it) {
    curIndex = *it;

    // key Existing in the path, Clone.
    if (parent->children_.count(curIndex)) {
      cursorRoot = shared_ptr<TrieNode>(std::move(parent->children_[curIndex])->Clone());
      parent->children_[curIndex] = cursorRoot;
      parent = cursorRoot;
    } else {
      // Not Existing in the path, Create new Node, add into current parent.
      cursorRoot = make_shared<TrieNode>();
      parent->children_[curIndex] = cursorRoot;
      parent = cursorRoot;
    }
  }

  // parent points to the second to last node.
  if (parent->children_.count(key.back())) {
    auto valueNode = parent->children_[key.back()];
    // Change ValueNode into TrieNodeWithValueType.
    parent->children_[key.back()] =
        make_shared<const TrieNodeWithValue<T>>(valueNode->children_, make_shared<T>(std::move(value)));
  } else {
    // Create TrieNodeValue with default constructor.
    parent->children_[key.back()] = make_shared<const TrieNodeWithValue<T>>(make_shared<T>(std::move(value)));
  }
  return Trie{newRoot};
}  // namespace bustub

// No-Recursive version
auto Trie::Remove(std::string_view key) const -> Trie {
  // throw NotImplementedException("Trie::Remove is not implemented.");
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  if (!root_) return *this;

  if (key.empty()) {
    if (root_->is_value_node_) {
      // Convert from TrieNodeWithValue to TrieNode.
      auto newRoot = make_shared<TrieNode>(std::move(root_.get()->Clone()->children_));
      // Delete root_ node
      if (newRoot->children_.size() == 0) {
        return Trie{};
      } else {
        return Trie{newRoot};
      }
    }
  }

  // Copy-On_Write
  auto newRoot = shared_ptr<TrieNode>(root_->Clone());

  shared_ptr<TrieNode> parent = newRoot;
  shared_ptr<TrieNode> curRoot{};
  std::vector<std::pair<shared_ptr<TrieNode>, char>> vec{};
  // (parent0, keyFromParent0),(parent1, keyFromParent1)
  bool found = true;

  char curIndex;
  // Internal node search.
  for (auto it = key.begin(); it != key.end() - 1; it++) {
    curIndex = *it;
    vec.push_back(std::make_pair(parent, *it));
    // key Existing in the path, Clone.
    if (parent->children_.count(curIndex)) {
      // curRoot = make_shared<TrieNode>(std::move(parent->children_[curIndex]->Clone()->children_));
      curRoot = shared_ptr<TrieNode>(parent->children_[curIndex]->Clone());
      parent->children_[curIndex] = curRoot;
      parent = curRoot;
    } else {
      found = false;
      break;
    }
  }
  if (!found) {
    return *this;
  } else {
    // Check last character.
    char lastc = key.back();
    // Last char not found
    if (!parent->children_.count(lastc)) {
      return *this;
    } else {
      // Last node is not a value node.
      auto lastNode = parent->children_[lastc];
      if (lastNode->is_value_node_ == false) {
        return *this;
      } else {
        vec.push_back(std::make_pair(parent, lastc));
        // Convert from TrieNodeWithValue to TrieNode Type.
        // Copy-On-Write
        auto newNode =
            make_shared<TrieNode>(std::move((dynamic_cast<const TrieNode *>(lastNode.get())->Clone()->children_)));

        // If this is the last usage.
        if (newNode->children_.size() == 0) {
          int cnt = 0;
          std::vector<std::pair<shared_ptr<TrieNode>, char>>::reverse_iterator parent;
          for (parent = vec.rbegin(); cnt == 0 && parent != vec.rend(); ++parent) {
            parent->first->children_.erase(parent->second);

            if (parent->first->is_value_node_) {
              break;
            }
            cnt = parent->first->children_.size();
          }
          // If root is empty.
          if (cnt == 0 && parent == vec.rend()) {
            return Trie{};
          }
        } else {
          parent->children_[lastc] = newNode;
        }
        return Trie{newRoot};
      }
    }
  }
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked
// up by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
