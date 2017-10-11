/**
 * transaction.h
 */

#pragma once

#include <atomic>
#include <memory>
#include <unordered_map>

#include "common/config.h"
#include "page/page.h"

namespace cmudb {
class Transaction {
public:
  Transaction(Transaction const &) = delete;
  Transaction(const size_t &thread_id, const txn_id_t &txn_id)
      : thread_id_(thread_id), txn_id_(txn_id) {
    // TODO!!
    page_set_.reset();
  }
  ~Transaction() {}
  //===--------------------------------------------------------------------===//
  // Mutators and Accessors
  //===--------------------------------------------------------------------===//
  inline size_t GetThreadId() const { return thread_id_; }

  inline txn_id_t GetTransactionId() const { return txn_id_; }

  inline std::shared_ptr<std::unordered_map<page_id_t, Page *>> GetPageSet() {
    return page_set_;
  }

private:
  // thread id
  size_t thread_id_;
  // transaction id
  txn_id_t txn_id_;
  // this set contains map from page_id to page pointer that was latched
  // during index operation
  std::shared_ptr<std::unordered_map<page_id_t, Page *>> page_set_;
};
} // namespace cmudb
