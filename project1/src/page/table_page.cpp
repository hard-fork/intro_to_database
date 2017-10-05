/**
 * header_page.cpp
 */

#include <cassert>

#include "page/table_page.h"

namespace cmudb {
/**
 * Header related
 */
void TablePage::Init(page_id_t page_id, size_t page_size,
                     page_id_t prev_page_id, page_id_t next_page_id) {
  memcpy(GetData(), &page_id, 4); // set page_id
  SetPrevPageId(prev_page_id);
  SetNextPageId(next_page_id);
  SetFreeSpacePointer(page_size);
  SetTupleCount(0);
}

page_id_t TablePage::GetPageId() {
  return *reinterpret_cast<page_id_t *>(GetData());
}

page_id_t TablePage::GetPrevPageId() {
  return *reinterpret_cast<page_id_t *>(GetData() + 4);
}

page_id_t TablePage::GetNextPageId() {
  return *reinterpret_cast<page_id_t *>(GetData() + 8);
}

void TablePage::SetPrevPageId(page_id_t prev_page_id) {
  memcpy(GetData() + 4, &prev_page_id, 4);
}

void TablePage::SetNextPageId(page_id_t next_page_id) {
  memcpy(GetData() + 8, &next_page_id, 4);
}

/**
 * Tuple related
 */
bool TablePage::InsertTuple(const Tuple &tuple, RID &rid) {
  assert(tuple.allocated_);
  assert(tuple.size_ > 0);
  if (GetFreeSpaceSize() < tuple.size_) {
    return false; // not enough space
  }

  // try to reuse a free slot first
  int i;
  for (i = 0; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) <= 0) { // empty slot
      break;
    }
  }

  // no free slot left
  if (i == GetTupleCount() && GetFreeSpaceSize() < tuple.size_ + 8) {
    return false; // not enough space
  }

  SetFreeSpacePointer(GetFreeSpacePointer() -
                      tuple.size_); // update free space pointer first
  memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
  SetTupleOffset(i, GetFreeSpacePointer());
  SetTupleSize(i, tuple.size_);
  if (i == GetTupleCount())
    SetTupleCount(GetTupleCount() + 1);
  rid.Set(GetPageId(), i);
  return true;
}

bool TablePage::DeleteTuple(const RID &rid) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount())
    return false;
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size <= 0)
    return false;
  int32_t tuple_offset =
      GetTupleOffset(slot_num); // the tuple offset of the deleted tuple
  int32_t free_space_pointer =
      GetFreeSpacePointer(); // old pointer to the free space
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size);
  SetTupleSize(slot_num, 0);
  SetTupleOffset(slot_num, 0); // invalid offset
  for (int i = 0; i < GetTupleCount(); ++i) {
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset) {
      SetTupleOffset(i, tuple_offset_i + tuple_size);
    }
  }
  return true;
}

bool TablePage::UpdateTuple(const Tuple &tuple, const RID &rid) {
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount())
    return false;
  int32_t tuple_size = GetTupleSize(slot_num); // old tuple size
  if (tuple_size <= 0 || GetFreeSpaceSize() < tuple.size_ - tuple_size)
    return false;
  int32_t tuple_offset =
      GetTupleOffset(slot_num); // the tuple offset of the old tuple
  int32_t free_space_pointer =
      GetFreeSpacePointer(); // old pointer to the free space
  assert(tuple_offset >= free_space_pointer);
  memmove(GetData() + free_space_pointer + tuple_size - tuple.size_,
          GetData() + free_space_pointer, tuple_offset - free_space_pointer);
  SetFreeSpacePointer(free_space_pointer + tuple_size - tuple.size_);
  memcpy(GetData() + tuple_offset + tuple_size - tuple.size_, tuple.data_,
         tuple.size_);                 // copy new tuple
  SetTupleSize(slot_num, tuple.size_); // update tuple size in slot
  for (int i = 0; i < GetTupleCount();
       ++i) { // update tuple offsets (including the updated one)
    int32_t tuple_offset_i = GetTupleOffset(i);
    if (GetTupleSize(i) > 0 && tuple_offset_i < tuple_offset + tuple_size) {
      SetTupleOffset(i, tuple_offset_i + tuple_size - tuple.size_);
    }
  }
  return true;
}

bool TablePage::GetTuple(const RID &rid, Tuple &tuple) {
  assert(!tuple.allocated_);
  int slot_num = rid.GetSlotNum();
  if (slot_num >= GetTupleCount())
    return false;
  int32_t tuple_size = GetTupleSize(slot_num);
  if (tuple_size <= 0)
    return false;
  int32_t tuple_offset = GetTupleOffset(slot_num);
  tuple.size_ = tuple_size;
  tuple.data_ = GetData() + tuple_offset;
  tuple.rid_ = rid;
  return true;
}

/**
 * Tuple iterator
 */
bool TablePage::GetFirstTupleRid(RID &first_rid) {
  for (int i = 0; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      first_rid.Set(GetPageId(), i);
      return true;
    }
  }
  // there is no tuple within current page
  first_rid.Set(INVALID_PAGE_ID, -1);
  return false;
}

bool TablePage::GetNextTupleRid(const RID &cur_rid, RID &next_rid) {
  assert(cur_rid.GetPageId() == GetPageId());
  for (auto i = cur_rid.GetSlotNum() + 1; i < GetTupleCount(); ++i) {
    if (GetTupleSize(i) > 0) { // valid tuple
      next_rid.Set(GetPageId(), i);
      return true;
    }
  }
  return false; // End of last tuple
}

/**
 * helper functions
 */

// tuple slots
int32_t TablePage::GetTupleOffset(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 20 + 8 * slot_num);
}

int32_t TablePage::GetTupleSize(int slot_num) {
  return *reinterpret_cast<int32_t *>(GetData() + 24 + 8 * slot_num);
}

void TablePage::SetTupleOffset(int slot_num, int32_t offset) {
  memcpy(GetData() + 20 + 8 * slot_num, &offset, 4);
}

void TablePage::SetTupleSize(int slot_num, int32_t offset) {
  memcpy(GetData() + 24 + 8 * slot_num, &offset, 4);
}

// free space
int32_t TablePage::GetFreeSpacePointer() {
  return *reinterpret_cast<int32_t *>(GetData() + 12);
}

void TablePage::SetFreeSpacePointer(int32_t free_space_pointer) {
  memcpy(GetData() + 12, &free_space_pointer, 4);
}

// tuple count
int32_t TablePage::GetTupleCount() {
  return *reinterpret_cast<int32_t *>(GetData() + 16);
}

void TablePage::SetTupleCount(int32_t tuple_count) {
  memcpy(GetData() + 16, &tuple_count, 4);
}

// for free space calculation
int32_t TablePage::GetFreeSpaceSize() {
  return GetFreeSpacePointer() - 20 - GetTupleCount() * 8;
}
} // namespace cmudb
