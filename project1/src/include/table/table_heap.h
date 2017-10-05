/**
 * table_heap.h
 *
 * doubly-linked list of heap pages
 */

#pragma once

#include "buffer/buffer_pool_manager.h"
#include "table/tuple.h"
#include "table/table_iterator.h"
#include "page/table_page.h"

namespace cmudb {

class TableHeap {
  friend class TableIterator;

public:
  ~TableHeap() {
    // when destruct table heap, flush all pages within buffer pool
    buffer_pool_manager_->FlushAllPages();
  }

  // open/create a table heap, create table if first_page_id is not passed
  TableHeap(BufferPoolManager *buffer_pool_manager,
            page_id_t first_page_id = INVALID_PAGE_ID);

  // for insert, if tuple is too large (>~page_size), return false
  bool InsertTuple(const Tuple &tuple, RID &rid);

  bool DeleteTuple(const RID &rid);

  // if the new tuple is too large to fit in the old page, return false (will
  // delete and insert)
  bool UpdateTuple(const Tuple &tuple, const RID &rid);

  bool GetTuple(const RID &rid, Tuple &tuple);

  bool DeleteTableHeap();

  TableIterator begin();

  TableIterator end();

  inline page_id_t GetFirstPageId() const { return first_page_id_; }

private:
  /**
   * Members
   */
  BufferPoolManager *buffer_pool_manager_;
  page_id_t first_page_id_;
};

} // namespace cmudb
