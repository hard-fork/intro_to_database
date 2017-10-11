#include <list>
#include <mutex>
#include <utility>
#include <functional>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

template <typename K, typename V>
Bucket<K, V>::Bucket(size_t size, int d){
  depth = d;
  bucket_size = size;
}

template <typename K, typename V>
bool Bucket<K, V>::IsFull(){
  return items.size() >= bucket_size;
}

template <typename K, typename V>
bool Bucket<K, V>::Find(const K &key, V &value){
  for(std::pair<K, V> item : items){
    if(item.first == key){
      value = item.second;
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
void Bucket<K, V>::Insert(const K &key, const V &value){
  items.push_back(std::make_pair(key, value));
}

template <typename K, typename V>
bool Bucket<K, V>::Remove(const K &key){
  for(auto it = items.begin(); it != items.end(); it ++){
    if(it -> first == key){
      std::lock_guard<std::mutex> lock(mtx);

      items.erase(it);
      return true;
    }
  }

  return false;
}

template <typename K, typename V>
size_t Bucket<K, V>::HashCode(const K &key){
  return std::hash<K>{}(key);
}

template <typename K, typename V>
std::pair<Bucket<K, V>, Bucket<K, V>> Bucket<K, V>::Split(){
  depth ++;

  Bucket<K, V> b0(bucket_size, depth);
  Bucket<K, V> b1(bucket_size, depth);

  for(std::pair<K, V>& item : items){
    if(HashCode(item.first) & (1 << (depth - 1))){
      b1.items.push_back(item);
    } else {
      b0.items.push_back(item);
    }
  }

  return std::make_pair(b0, b1);
}

template <typename K, typename V>
int Bucket<K, V>::GetLocalDepth(){
  return depth;
}

template <typename K, typename V>
void Bucket<K, V>::SetDepth(int d){
  depth = d;
}

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) {
  depth = 1;
  bucket_num = 2;
  bucket_size = size;

  Bucket<K, V> b1(size, depth);
  Bucket<K, V> b2(size, depth);
  buckets.push_back(&b1);
  buckets.push_back(&b2);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  return std::hash<K>{}(key) & ((1 << depth) - 1);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return depth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return buckets[bucket_id] -> GetLocalDepth();
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return bucket_num;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  return buckets[HashKey(key)] -> Find(key, value);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  return buckets[HashKey(key)] -> Remove(key);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  size_t hash_addr = HashKey(key);
  Bucket<K, V>* b = buckets[hash_addr];

  if(b -> IsFull()){
    b -> Insert(key, value);

    std::lock_guard<std::mutex> lock(mtx);

    std::pair<Bucket<K, V>, Bucket<K, V>> s_buckets = b -> Split();
    bucket_num ++;

    if(b -> GetLocalDepth() > depth){
      depth ++;

      // double buckets vector
      auto begin = buckets.begin();
      auto end = buckets.end();
      while(begin != end){
        buckets.push_back(*begin);
        begin ++;
      }

      buckets[hash_addr] = &(s_buckets.first);
      buckets[hash_addr | (1 << (depth - 1))] = &(s_buckets.second);
    } else{
      int local_depth = b -> GetLocalDepth();
      int top_bit = 1 << (local_depth - 1);
      int mask = top_bit - 1;

      size_t mask_addr = hash_addr & mask;

      for(int i = 0; i < buckets.size(); i ++){
        if((i & mask) == mask_addr){
          if(i & top_bit){
            buckets[i] = &(s_buckets.second);
          } else {
            buckets[i] = &(s_buckets.first);
          }
        }
      }
    }
  } else{
    b -> Insert(key, value);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
