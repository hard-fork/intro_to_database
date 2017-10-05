/**
 * table_iterator.cpp
 */

#include <cassert>

#include "table/table_heap.h"
#include "table/table_iterator.h"

namespace cmudb {

TableIterator::TableIterator(TableHeap *table_heap, RID rid)
    : table_heap_(table_heap), tuple_(new Tuple(rid)) {
  if (rid.GetPageId() != INVALID_PAGE_ID) {
    table_heap_->GetTuple(tuple_->rid_, *tuple_);
  }
};

const Tuple &TableIterator::operator*() { // return reference to the table tuple
  assert(*this != table_heap_->end());
  return *tuple_;
}

Tuple *TableIterator::operator->() {
  assert(*this != table_heap_->end());
  return tuple_;
}

TableIterator &TableIterator::operator++() {
  BufferPoolManager *buffer_pool_manager = table_heap_->buffer_pool_manager_;
  auto cur_page = static_cast<TablePage *>(
      buffer_pool_manager->FetchPage(tuple_->rid_.GetPageId()));
  assert(cur_page != nullptr); // all pages are pinned

  RID next_tuple_rid;
  if (!cur_page->GetNextTupleRid(tuple_->rid_,
                                 next_tuple_rid)) { // end of this page
    if (cur_page->GetNextPageId() != INVALID_PAGE_ID) {
      auto next_page = static_cast<TablePage *>(
          buffer_pool_manager->FetchPage(cur_page->GetNextPageId()));
      // return value could be false
      // when you delete a tuple from certain page and no tuples remain valid
      // within that page
      next_page->GetFirstTupleRid(next_tuple_rid);
      buffer_pool_manager->UnpinPage(cur_page->GetNextPageId(), false);
    } else {
      next_tuple_rid.Set(INVALID_PAGE_ID, -1); // EOF
    }
  }
  buffer_pool_manager->UnpinPage(tuple_->rid_.GetPageId(), false);
  tuple_->rid_ = next_tuple_rid;

  if (*this != table_heap_->end()) {
    table_heap_->GetTuple(tuple_->rid_, *tuple_);
  }
  return *this;
}

TableIterator TableIterator::operator++(int) {
  TableIterator clone(*this);
  ++(*this);
  return clone;
}

} // namespace cmudb
