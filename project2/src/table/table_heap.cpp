/**
 * table_heap.cpp
 */

#include <cassert>

#include "common/logger.h"
#include "table/table_heap.h"

namespace cmudb {

TableHeap::TableHeap(BufferPoolManager *buffer_pool_manager,
                     page_id_t first_page_id)
    : buffer_pool_manager_(buffer_pool_manager), first_page_id_(first_page_id) {
  if (first_page_id_ == INVALID_PAGE_ID) {
    auto first_page =
        static_cast<TablePage *>(buffer_pool_manager_->NewPage(first_page_id_));
    assert(first_page != nullptr); // todo: abort table creation?
    LOG_DEBUG("new table page created %d", first_page_id_);

    first_page->Init(first_page_id_, PAGE_SIZE);
    buffer_pool_manager_->UnpinPage(first_page_id_, true);
  }
}

bool TableHeap::InsertTuple(const Tuple &tuple, RID &rid) {
  if (tuple.size_ + 28 > PAGE_SIZE) // larger than one page size
    return false;

  auto cur_page =
      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));

  while (!cur_page->InsertTuple(
      tuple, rid)) { // fail to insert due to not enough space
    auto next_page_id = cur_page->GetNextPageId();
    if (next_page_id != INVALID_PAGE_ID) { // valid next page
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), false);
      cur_page = static_cast<TablePage *>(
          buffer_pool_manager_->FetchPage(next_page_id));
    } else { // create new page
      auto new_page =
          static_cast<TablePage *>(buffer_pool_manager_->NewPage(next_page_id));
      assert(new_page != nullptr); // todo: abort when all pages are pinned?
      // std::cout << "new table page " << next_page_id << " created" <<
      // std::endl;
      cur_page->SetNextPageId(next_page_id);
      new_page->Init(next_page_id, PAGE_SIZE, cur_page->GetPageId(),
                     INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
      cur_page = new_page;
    }
  }
  buffer_pool_manager_->UnpinPage(cur_page->GetPageId(), true);
  return true;
}

bool TableHeap::DeleteTuple(const RID &rid) {
  // todo: remove empty page
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
  bool is_deleted = page->DeleteTuple(rid);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_deleted);
  return is_deleted;
}

bool TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid) {
  auto page = reinterpret_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
  bool is_updated = page->UpdateTuple(tuple, rid);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), is_updated);
  return is_updated;
}

bool TableHeap::GetTuple(const RID &rid, Tuple &tuple) {
  auto page = static_cast<TablePage *>(
      buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr)
    return false;
  bool res = page->GetTuple(rid, tuple);
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return res;
}

bool TableHeap::DeleteTableHeap() {
  // todo: real delete
  return true;
}

TableIterator TableHeap::begin() {
  auto page =
      static_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RID rid; // if failed (no tuple), rid will be the result of default
  // constructor, which means eof
  page->GetFirstTupleRid(rid);
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return TableIterator(this, rid);
}

TableIterator TableHeap::end() {
  return TableIterator(this, RID(INVALID_PAGE_ID, -1));
}

} // namespace cmudb
