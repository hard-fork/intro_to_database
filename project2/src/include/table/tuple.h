/**
 * tuple.h
 *
 * Tuple format:
 *  ------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD|
 *  ------------------------------------------------------------------
 */

#pragma once

#include "catalog/schema.h"
#include "common/rid.h"
#include "type/value.h"

namespace cmudb {

class Tuple {
  friend class TablePage;

  friend class TableHeap;

  friend class TableIterator;

public:
  // constructor for pointing to table heap
  Tuple(RID rid) : allocated_(false), rid_(rid) {}

  // constructor for creating a new tuple based on input value
  Tuple(std::vector<Value> values, Schema *schema);

  ~Tuple() {
    // std::cout << "rid is " << rid_.ToString();
    // std::cout << "is allocated " << allocated_ << '\n';
    if (allocated_)
      delete[] data_;
  }

  // return RID of current tuple
  inline RID GetRid() const { return rid_; }

  // Get the address of this tuple in the table's backing store
  inline char *GetData() const { return data_; }

  // Get length of the tuple, including varchar legth
  inline int32_t GetLength() const { return size_; }

  // Get the value of a specified column (const)
  // checks the schema to see how to return the Value.
  Value GetValue(Schema *schema, const int column_id) const;

  // Is the column value null ?
  inline bool IsNull(Schema *schema, const int column_id) const {
    Value value = GetValue(schema, column_id);
    return value.IsNull();
  }

  std::string ToString(Schema *schema) const;

private:
  // Get the starting storage address of specific column
  const char *GetDataPtr(Schema *schema, const int column_id) const;

  bool allocated_; // is allocated outside or is pointed to table heap?
  RID rid_;        // if pointing to the table heap, the rid is valid
  int32_t size_;
  char *data_;
};

} // namespace cmudb
