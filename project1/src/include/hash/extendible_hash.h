/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>
#include <utility>
#include <mutex>

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class Bucket{
public:
  Bucket(size_t size, int d);

  bool IsFull();
  bool Find(const K &key, V &value);
  bool Remove(const K &key);
  void Insert(const K &key, const V &value);

  std::pair<Bucket<K, V>, Bucket<K, V>> Split();
  size_t HashCode(const K &key);

  int GetLocalDepth();
  void SetDepth(int d);

private:
  std::vector<std::pair<K,V>> items;

  std::mutex mtx;

  int depth;
  size_t bucket_size;
};

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  // add your own member variables here
  std::vector<Bucket<K, V>*> buckets;

  std::mutex mtx;

  int depth;
  int bucket_num;
  size_t bucket_size;
};
} // namespace cmudb
